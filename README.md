# MLCacheDirect README

## 1. 项目简介

`MLCacheDirect` 是一个基于 **异步流水分片传输库** ，当前核心产物是：

- 动态库：`libos_transport.so`
- 对外头文件：`include/os_transport.h`

它的主要职责是：

1. 把一次大数据传输按固定大小切分为多个 chunk。
2. 通过 URMA 提交 `write with notify` 或 `recv` 请求。
3. 用线程池按 `request_id` 组织同一批 chunk 的后续处理。
4. 在收到 completion 后，唤醒对应 worker 推进同一请求的后续任务。
5. 为调用方提供一套“提交请求 -> 等待整批完成 -> 失败时取消剩余任务”的同步语义。

---

## 2. 当前目录结构

```text
.
├── BUILD.bazel
├── CMakeLists.txt
├── WORKSPACE
├── build.sh
├── build_bazel.sh
├── include/
│   ├── os_transport.h
│   ├── os_transport_internal.h
│   ├── os_transport_log_internal.h
│   ├── os_transport_thread_pool.h
│   ├── os_transport_thread_pool_internal.h
│   └── os_transport_urma.h
├── rpm/
│   └── os-transport.spec
├── src/
│   ├── os_transport.c
│   ├── os_transport_log.c
│   ├── os_transport_thread_pool.c
│   └── os_transport_urma.c
├── test/
│   ├── test_os_transport_unit.c
│   └── test_thread_pool.c
├── third_party/
│   ├── BUILD.bazel
│   └── BUILD.urma
└── tools/
    └── datasystem_test/
        ├── CMakeLists.txt
        ├── pipeline_h2d.cpp
        └── README.md
```

说明：

- 根目录下的库代码 **不依赖 CUDA runtime**。
- `tools/datasystem_test/pipeline_h2d.cpp` 是上层联调用例，里面仍然会使用 `cudaMalloc/cudaMemcpy`，用于验证“上层自己完成 H2D”的新模式。

---

## 3. 架构与职责边界

### 3.1 当前库负责什么

当前 `MLCacheDirect` 库负责：

- chunk 切分
- URMA 请求投递
- completion 事件解包
- 根据 `request_id` 唤醒线程池 worker
- 在 worker 中执行 send 侧的下一片发送任务
- 在 worker 中执行 recv 侧的上层回调 `notify_callback`
- 请求级同步与资源回收

### 3.2 当前库不再负责什么

当前库**不再直接负责**：

- `cudaSetDevice`
- `cudaMalloc`
- `cudaMemcpy`
- `cudaEventRecord`
- CUDA stream / event 生命周期管理

这些动作现在应该由上层完成，例如：

- SDK
- datasystem client
- 业务自己的 GPU 数据搬运模块

---

## 4. 核心数据结构

### 4.1 `os_transport_user_data_t`

定义在 `include/os_transport.h`：

```c
typedef union {
    struct {
        uint64_t chunk_type : 2;
        uint64_t chunk_id : 6;
        uint64_t chunk_size : 24;
        uint64_t request_id : 32;
    } bs;
    uint64_t user_ctx;
} os_transport_user_data_t;
```

这是库中最关键的 completion 上下文，编码了：

- `request_id`：整批请求的唯一标识
- `chunk_id`：当前是第几个 chunk
- `chunk_size`：当前 chunk 的字节数
- `chunk_type`：当前 chunk 的类型
  - `NOT_SPLIT`
  - `MIDDLE_CHUNK`
  - `LAST_CHUNK`

在 send 路径里，这份信息会被塞进 URMA 的 `notify_data` / `user_ctx`。  
在 recv 路径里，`os_transport_wake_up_task()` 会从 completion 中把它解析出来，再透传给上层回调。

### 4.2 `ost_buffer_info_t`

```c
typedef struct {
    uint64_t addr;
    urma_target_seg_t *tseg;
} ost_buffer_info_t;
```

表示一段 Host 侧缓冲区：

- `addr`：缓冲区地址
- `tseg`：URMA 目标段信息

### 4.3 `ost_device_info_t`

```c
typedef struct {
    urma_jfr_t *jfr;
    void *dst;
} ost_device_info_t;
```

虽然结构体名还叫 `device_info`，但就当前代码来说，它只为 recv 路径提供：

- `jfr`：用于提交 URMA recv 的接收端资源
- `dst`：目标地址

注意：当前库里**不会直接对 `dst` 做 `cudaMemcpy`**。  
它只是参与 chunk 地址计算，并在后续回调链路里让上层知道“这批数据是发往哪个目标地址逻辑范围的”。

### 4.4 `notify_callback_t`

```c
typedef int (*notify_callback_t)(void *user_data);
```

这是当前 recv 路径的关键扩展点。

回调参数 `user_data` 实际上指向的是一份 `os_transport_user_data_t` 副本，副本内容由 `os_transport_wake_up_task()` 解析 completion 后，在 worker 执行前填充。

也就是说，上层在 `notify_callback` 里可以拿到：

- `request_id`
- `chunk_id`
- `chunk_size`
- `chunk_type`

然后自行决定后续动作，例如：

- 查到对应 chunk 的 host buffer / device ptr
- 触发 `cudaMemcpyAsync`
- 做最后一个 chunk 的收尾通知

---

## 5. 代码模块说明

### 5.1 `src/os_transport.c`

这是库的主入口文件，主要负责：

- 对外 API 实现
- 参数校验
- 分片
- 构造任务组
- 注册到线程池
- completion 后的 worker 执行逻辑
- 等待请求完成并释放资源

其中最关键的两类 worker 任务是：

#### send worker

`send_task_worker_func()` 负责调用：

- `urma_write_with_notify()`

把后续 chunk 继续发出去。

#### recv worker

`recv_task_worker_func()` 当前**不做 CUDA 拷贝**，而是：

1. 从 `recv_task_arg->notify_user_data` 取出 completion 元数据；
2. 调用上层注册的 `notify_callback(&recv_task_arg->notify_user_data)`；
3. 根据回调返回值决定当前 chunk 是否成功；
4. 更新整批任务的完成计数。

这正是“把 `cudaMemcpy` 移出去”后的核心落点。

### 5.2 `src/os_transport_thread_pool.c`

这是线程池实现，负责：

- worker 创建与销毁
- 任务队列管理
- pending request 队列管理
- `request_id -> worker` 绑定
- 收到 completion 后按 `request_id` 唤醒 worker
- 批量任务提交
- 按请求取消未执行任务

当前设计的关键点：

1. **同一个 `request_id` 的任务会绑定到同一个 worker。**
2. **worker 真正执行任务的时机由 completion 唤醒推进。**
3. **整批任务共享同一个 `task_sync_t`，主线程可以统一等待。**

### 5.3 `src/os_transport_urma.c`

负责 URMA 封装：

- `urma_write_with_notify()`
- `urma_recv_with_notify()`

send 路径通过 `URMA_OPC_WRITE_IMM` 发送，并携带 `notify_data` / `user_ctx`。  
recv 路径负责向 `jfr` 投递 recv work request。

### 5.4 `src/os_transport_log.c`

负责日志注册与日志输出。

对外接口：

```c
int os_transport_log_reg(int level, log_callback_t cb);
```

---

## 6. 分片机制

默认 chunk 大小定义在 `include/os_transport.h`：

```c
#define DEFAULT_CHUNK_SIZE (2 * 1024 * 1024)
```

即默认 **2MB**。

### 6.1 send / recv 的共同分片规则

- 当 `len <= DEFAULT_CHUNK_SIZE` 时，按单 chunk 处理。
- 当 `len > DEFAULT_CHUNK_SIZE` 时，拆成多个 chunk。
- 每个 chunk 都会有自己的 `chunk_id`、`chunk_size` 和 `chunk_type`。

### 6.2 send 路径分片特点

大于 2MB 时：

1. 主线程先把整批 send task 注册到线程池；
2. 立即手动发送第一个 chunk；
3. 后续 chunk 由 completion 驱动 worker 逐个继续发送；
4. 调用方通过 `wait_and_free_sync()` 等待整批完成。

### 6.3 recv 路径分片特点

大于 2MB 时：

1. 主线程先切出所有 chunk；
2. 为每个 chunk 注册一个 recv task；
3. 调用 `urma_recv_with_notify()` 为每个 chunk 投递 recv 请求；
4. completion 到来后，线程池按 `request_id` 唤醒对应 worker；
5. worker 调用上层 `notify_callback`；
6. 调用方通过 `wait_and_free_sync()` 等待整批完成。

这里要特别注意：

> recv 路径的“完成”语义，当前指的是：
> - URMA completion 已到达；
> - 对应的 `notify_callback` 已执行完成并返回。
>
> 它**不自动等价于**“GPU 数据已经就绪”，除非你的 `notify_callback` 自己实现了这部分保证。

---

## 7. 对外接口说明

### 7.1 初始化

```c
uint32_t os_transport_init(urma_context_t *urma_ctx,
                           os_transport_cfg_t *ost_cfg,
                           void **handle);
```

作用：

- 初始化 `os_transport_handle_t`
- 保存 URMA 上下文和配置
- 创建线程池
- 注册 JFC/JFCE
- 启动线程池

关键配置项：

```c
typedef struct os_transport_cfg {
    bool urma_event_mode;
    uint8_t reserved1[3];
    uint32_t worker_thread_num;
    urma_jfce_t *jfce;
    urma_jfc_t *jfc;
    uint32_t reserved2[10];
} os_transport_cfg_t;
```

### 7.2 注册日志

```c
int os_transport_log_reg(int level, log_callback_t cb);
```

用于注册日志回调。

### 7.3 send 接口

```c
uint32_t os_transport_send(void *handle,
                           urma_jetty_info_t *jetty_info,
                           ost_buffer_info_t *local_src,
                           ost_buffer_info_t *remote_dst,
                           uint32_t len,
                           uint32_t server_key,
                           uint32_t client_key,
                           task_sync_t **ret_sync_handle);
```

作用：

- 切分 Host -> Host 发送数据
- 生成异步 send task
- 首片立即发送
- 后续分片由 notify 驱动 worker 继续发送
- 返回 `task_sync_t` 供调用方等待整批完成

说明：

- `client_key` 最终会作为 `request_id` 写入 completion 透传字段。
- `server_key` 会用于另一侧 completion/上下文区分。

### 7.4 recv 接口

```c
uint32_t os_transport_recv(void *handle,
                           ost_buffer_info_t *host_src,
                           ost_device_info_t *device_dst,
                           uint32_t len,
                           uint32_t client_key,
                           task_sync_t **ret_sync_handle,
                           notify_callback_t notify_callback);
```

作用：

- 切分接收数据范围
- 为每个 chunk 注册 recv task
- 向 URMA 的 `jfr` 投递 recv 请求
- completion 到来后回调上层 `notify_callback`
- 返回 `task_sync_t` 给调用方等待整批完成

这个接口和旧版最大的不同是：

> `notify_callback` 现在是 **必填**。  
> 库自身不再在 recv worker 中做 `cudaMemcpy`，而是把“收到 completion 之后怎么处理这片数据”的决定权交给上层。

### 7.5 completion 唤醒接口

```c
int os_transport_wake_up_task(void *handle, void *cr_t);
```

作用：

- 从 URMA completion 中解析 `user_data`
- 获取 `request_id`
- 通知线程池唤醒对应 worker
- 把本次 completion 的 `user_data` 作为透传信息传给后续 recv worker

### 7.6 等待并释放同步资源

```c
uint32_t wait_and_free_sync(void *handle, task_sync_t *sync_handle);
```

作用：

- 等待当前请求整批任务结束
- 如果中途检测到任务未完整完成，则按 `request_id` 取消剩余任务
- 释放同步对象、chunk 数组和任务组资源

### 7.7 取消指定请求的任务

```c
uint32_t os_transport_cancel_tasks(void *handle, uint32_t request_id);
```

作用：

- 取消该 `request_id` 对应的未执行任务

### 7.8 销毁句柄

```c
uint32_t os_transport_destroy(void *handle);
```

作用：

- 销毁线程池
- 释放内部资源

---

## 8. 典型时序

### 8.1 send 路径时序

```text
调用方
  -> os_transport_send()
      -> 切 chunk
      -> 注册后续 task 到线程池
      -> 立即发送第一个 chunk

远端/本端产生 completion
  -> os_transport_wake_up_task()
      -> 按 request_id 唤醒 worker
          -> worker 执行下一片 send task
              -> urma_write_with_notify()

调用方
  -> wait_and_free_sync()
      -> 等整批发送完成
```

### 8.2 recv 路径时序

```text
调用方
  -> os_transport_recv(..., notify_callback)
      -> 切 chunk
      -> 注册 recv task
      -> 为每个 chunk 投递 urma_recv_with_notify()

URMA completion 到来
  -> os_transport_wake_up_task()
      -> 解析 request_id/chunk_id/chunk_size/chunk_type
      -> 按 request_id 唤醒 worker
          -> worker 执行 recv task
              -> notify_callback(&os_transport_user_data_t_copy)

上层 notify_callback
  -> 根据 chunk_id / chunk_size 查找业务上下文
  -> 视需要执行 cudaMemcpy / cudaMemcpyAsync / 其他处理

调用方
  -> wait_and_free_sync()
      -> 等整批 recv task 完成
```

---

## 9. 如何在上层实现新的 H2D 模式

因为 `cudaMemcpy` 已经从库内移出，推荐上层把真正的 H2D 逻辑写进 `notify_callback` 或其后续调度流程中。

一个最简伪代码如下：

```c
static int my_notify_callback(void *user_data)
{
    os_transport_user_data_t *ud = (os_transport_user_data_t *)user_data;
    if (!ud) {
        return -1;
    }

    uint32_t request_id = ud->bs.request_id;
    uint32_t chunk_id = ud->bs.chunk_id;
    uint32_t chunk_size = ud->bs.chunk_size;
    uint32_t chunk_type = ud->bs.chunk_type;

    // 1. 根据 request_id 找到这次请求在上层保存的上下文
    // 2. 根据 chunk_id 找到对应 host 地址和 device 地址
    // 3. 执行 cudaMemcpyAsync(...) 或其他处理
    // 4. 如有需要，在 LAST_CHUNK 时做额外收尾

    (void)chunk_size;
    (void)chunk_type;
    return 0;
}
```

建议上层自己维护一份“请求上下文表”，至少包含：

- `request_id`
- 原始 host base address
- 原始 device base address
- 每个 chunk 的偏移
- stream / event
- 是否最后一片
- 是否需要聚合完成通知

这样 `notify_callback` 才能真正把 completion 元数据映射成实际的 GPU 拷贝动作。

---

## 10. 构建说明

### 10.1 CMake 构建

项目根目录提供 `build.sh`：

```bash
chmod +x build.sh
./build.sh
```

仅运行单元测试：

```bash
./build.sh -t
```

当前 `build.sh` 特点：

- 自动识别 `x86_64/aarch64`
- 构建目录为 `build-<arch>`
- 默认会清理旧的 CMake/Bazel 构建产物
- 默认会生成 RPM

依赖：

- `cmake`
- `gcc`
- `make`
- `rpmbuild`（打包时）
- `liburma.so`

### 10.2 Bazel 构建

项目根目录提供 `build_bazel.sh`：

```bash
chmod +x build_bazel.sh
./build_bazel.sh
```

运行测试：

```bash
./build_bazel.sh -t
```

清理构建缓存：

```bash
./build_bazel.sh -c
```

可通过环境变量指定 URMA 路径：

```bash
URMA_INCLUDE_DIR=/usr/include \
URMA_LIB_DIR=/usr/lib64 \
./build_bazel.sh
```

### 10.3 当前库的构建依赖说明

根目录库本身当前只依赖：

- URMA
- pthread

**不依赖 CUDA runtime。**

这一点可以从当前代码直接看出：

- 根目录 `src/*.c` 没有包含 `cuda_runtime.h`
- `CMakeLists.txt` 没有链接 `CUDA::cudart`
- `BUILD.bazel` 没有引入 CUDA 依赖

只有 `tools/datasystem_test/pipeline_h2d.cpp` 这个上层联调用例还需要 CUDA。

---

## 11. RPM 产物

默认打包会生成：

- 主包：`os-transport-<version>-<release>.<arch>.rpm`
- 开发包：`os-transport-devel-<version>-<release>.<arch>.rpm`

安装内容主要包括：

- `libos_transport.so`
- `include/os_transport.h`

当前对外只安装公共头：

- `include/os_transport.h`

内部头文件不对外导出。

---

## 12. 测试

### 12.1 单元测试

当前仓库内置两个测试：

- `test_thread_pool`
- `test_os_transport_unit`

运行方式：

```bash
./build.sh -t
```

或：

```bash
./build_bazel.sh -t
```

### 12.2 上层联调工具

`tools/datasystem_test/pipeline_h2d.cpp` 是一个联调/验证程序。

它的意义在于：

- 演示上层如何自己分配 device memory
- 演示上层如何在业务层处理 H2D
- 验证“库只做传输与通知，上层做 CUDA 拷贝”的新分工

这个工具的编译和使用说明见：

- `tools/datasystem_test/README.md`

---

## 13. 使用建议与注意事项

### 13.1 recv 路径必须提供 `notify_callback`

当前 `os_transport_recv()` 的入参校验要求 `notify_callback != NULL`。

因为当前版本里，recv worker 的主要职责就是执行这个回调。

### 13.2 `wait_and_free_sync()` 只保证库侧任务完成

当前等待语义保证的是：

- URMA 任务提交与 completion 推进完成
- 线程池中对应 task 已完成
- 上层 `notify_callback` 已返回

它不天然保证：

- GPU kernel 已完成
- CUDA stream 已同步
- device 数据一定已经可立即消费

这些保证要由你的 `notify_callback` 及其外部同步逻辑补齐。

### 13.3 `request_id` 是整批请求的关键键值

无论 send 还是 recv，`request_id` 都是整个任务编排的核心索引。

上层最好保证：

- 同时活跃的请求中 `request_id` 唯一
- 能通过 `request_id` 快速查到业务上下文

### 13.4 如果回调要做重活，建议上层自行控制并发

当前 `notify_callback` 运行在线程池 worker 中。  
如果你在回调里直接做耗时很长的 H2D、同步等待或复杂业务逻辑，会占住该 worker。

因此更推荐的方式通常是：

- 在 `notify_callback` 里只做轻量分发
- 真正的 CUDA 操作交给上层自己的线程/stream 调度模块

---

## 14. 一句话总结

当前版本的 `MLCacheDirect` 已经从“库内部负责 URMA + CUDA memcpy”的模型，调整成了“库负责 **URMA 分片传输 + completion 唤醒 + 上层回调编排**，而 **H2D 具体执行由上层负责**”的模型。

因此，在接入这个版本时，最重要的不是再去找库里的 `cudaMemcpy`，而是要在上层把这三件事补齐：

1. `request_id -> 业务上下文` 映射；
2. `chunk_id -> host/device 偏移` 映射；
3. `notify_callback` 中的 H2D / 完成同步策略。


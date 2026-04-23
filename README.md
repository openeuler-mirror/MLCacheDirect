# MLCacheDirect 仓库说明

## 1. 仓库简介

`MLCacheDirect` 是一个基于 **URMA + CUDA Runtime** 的 C 语言传输库，核心目标是把远端主机上的数据通过流水线方式分片搬运到本端 Host，再从本端 Host 继续搬运到 GPU。

当前仓库的核心产物是：

- 动态库：`libos_transport.so`
- 对外头文件：`include/os_transport.h`

这个仓库当前已经同时支持：

- **CMake 构建**
- **Bazel 构建**
- **RPM 打包**

从当前代码实现来看，库的主要职责包括：

1. 基于 URMA 发起 Host 到 Host 的 write with notify。
2. 按固定 chunk 大小对大块数据进行分片。
3. 基于线程池按 `request_id` 组织任务执行。
4. 在本端收到通知后触发下一阶段任务推进。
5. 在本端把 Host 数据继续搬运到 GPU。

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
│   ├── os_transport_thread_pool.c
│   └── os_transport_urma.c
├── test/
│   ├── os_transport_sample.c
│   ├── test_os_transport_unit.c
│   ├── test_thread_pool.c
│   └── urma_sample.c
└── third_party/
    ├── BUILD.bazel
    ├── BUILD.cuda
    └── BUILD.urma
```

---

## 3. 核心模块说明

### 3.1 `src/os_transport.c`

这是主入口实现文件，负责：

- 对外 API 的实现
- send/recv 参数校验
- 大块数据分片
- 构造线程池任务
- 等待整批任务完成
- 失败时按 `request_id` 取消剩余任务

当前对外暴露的主要接口定义在 `include/os_transport.h` 中：

- `os_transport_init()`：初始化传输句柄和线程池
- `os_transport_reg_jfc()`：注册 JFC/JFCE 信息
- `os_transport_send()`：远端 Host -> 本端 Host 的发送入口
- `os_transport_recv()`：本端 Host -> GPU 的接收入口
- `os_transport_wake_up_task()`：收到通知后按 `request_id` 唤醒对应任务
- `wait_and_free_sync()`：等待一批任务全部结束并释放同步资源
- `os_transport_destroy()`：销毁库句柄和线程池

### 3.2 `src/os_transport_thread_pool.c`

这是线程池模块，负责：

- worker 创建与启动
- request 级别的任务归属
- 按 `request_id` 将任务绑定到固定 worker
- 收到通知后唤醒指定 request 的 worker
- 批量提交任务
- 取消指定 `request_id` 对应的未执行任务

当前实现里有几个很关键的设计点：

1. **同一批分片任务共享同一个 `request_id`**。
2. 同一个 `request_id` 会绑定到同一个 worker。
3. 收到 URMA 通知后，通过 `thread_pool_wake_up_worker_by_req_id()` 推进该请求的下一片任务。
4. 可以通过 `thread_pool_cancel_tasks_by_req()` 只清理某个请求的剩余任务，不影响其他请求。

### 3.3 `src/os_transport_urma.c`

这个文件负责与 URMA 相关的底层封装，当前主要用于 write with notify 以及相关数据结构处理。

---

## 4. 分片与流水线机制

当前实现中，默认 chunk 大小为：

```c
#define DEFAULT_CHUNK_SIZE (2 * 1024 * 1024)
```

也就是 **2MB**。

### 4.1 分片规则

当 `os_transport_send()` 或 `os_transport_recv()` 的数据长度：

- 小于等于 `DEFAULT_CHUNK_SIZE`：直接按单片处理
- 大于 `DEFAULT_CHUNK_SIZE`：拆成多个 chunk，每个 chunk 最多 2MB

### 4.2 分片上下文

`os_transport_user_data_t` 里封装了当前任务的重要上下文：

- `chunk_type`
- `chunk_id`
- `chunk_size`
- `request_id`

其中 `request_id` 是整个请求分片链路的关键标识。

### 4.3 当前流水线语义

结合当前代码和现有调用方式，可以把链路理解为：

1. 远端先发送第一个分片到本端 Host。
2. 第一个分片写入完成后，远端和本端都会收到通知。
3. 远端收到通知后，继续推进下一个分片写入。
4. 本端收到通知后，开始把当前 Host 分片搬运到 GPU。
5. 周而复始，直到同一个 `request_id` 对应的所有分片全部完成。

---

## 5. 对外接口说明

### 5.1 初始化

```c
uint32_t os_transport_init(urma_context_t *urma_ctx,
                           os_transport_cfg_t *ost_cfg,
                           void **handle);
```

作用：

- 初始化全局句柄
- 保存 URMA 上下文
- 初始化线程池
- 启动 worker 线程

关键配置项：

- `urma_event_mode`
- `worker_thread_num`
- `jfce`
- `jfc`

### 5.2 注册 JFC

```c
uint32_t os_transport_reg_jfc(urma_jfce_t *jfce,
                              urma_jfc_t *jfc,
                              void *handle);
```

作用：

- 更新库内部用于 poll/事件处理的 JFC/JFCE 信息。

### 5.3 发送

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

- 将本端 Host 缓冲区数据按 chunk 切分
- 构造 URMA write with notify 任务
- 将任务批量提交到线程池
- 返回 `task_sync_t` 用于后续等待整批完成

### 5.4 接收

```c
uint32_t os_transport_recv(void *handle,
                           ost_buffer_info_t *host_src,
                           ost_device_info_t *device_dst,
                           uint32_t len,
                           uint32_t client_key,
                           task_sync_t **ret_sync_handle);
```

作用：

- 将本端 Host 上的数据按 chunk 切分
- 构造 Host -> GPU 的搬运任务
- 批量提交到线程池
- 返回同步句柄

### 5.5 通知唤醒

```c
int os_transport_wake_up_task(void *handle, void *cr_t);
```

作用：

- 从完成事件里解析 `request_id`
- 调用线程池按 `request_id` 唤醒对应 worker
- 推进下一片任务执行

### 5.6 等待全部完成

```c
uint32_t wait_and_free_sync(void *handle, task_sync_t *sync_handle);
```

作用：

- 等待当前请求整批任务完成
- 如果中途有任务失败，会提前结束等待
- 释放本次请求关联的同步和任务组资源

### 5.7 销毁

```c
uint32_t os_transport_destroy(void *handle);
```

作用：

- 销毁线程池
- 释放库句柄
- 清理内部资源

---

## 6. 线程池与 request_id 机制

当前线程池 API 定义在 `include/os_transport_thread_pool.h`，其设计重点是：

- **任务调度不是纯 FIFO**
- 而是以 **`request_id` 为主线** 组织执行

### 6.1 request 绑定 worker

同一个 `request_id` 的任务会绑定到同一个 worker，保证：

- 同一请求的分片顺序推进
- 收到 notify 后能精确唤醒目标 worker

### 6.2 批量任务提交

批量提交接口：

```c
uint64_t *thread_pool_submit_batch_tasks(...)
```

要求一批任务中的 `request_id` 一致，否则会返回错误。

### 6.3 取消任务

取消接口：

```c
int thread_pool_cancel_tasks_by_req(ThreadPoolHandle handle, uint32_t request_id);
```

语义是：

- 只删除某个 `request_id` 对应的**未执行任务**
- 不会影响其他请求的任务

这个能力对于超时清理和失败回滚很重要。

---

## 7. 构建方式

当前仓库同时支持 CMake 和 Bazel。

### 7.1 CMake 构建

直接执行：

```bash
./build.sh
```

脚本会自动完成：

1. 架构识别（`x86_64` / `aarch64`）
2. 清理旧产物
3. CMake 配置
4. `make` 编译
5. 临时安装布局生成
6. RPM 打包

输出目录主要是：

- `build-<arch>/`
- `output/`

### 7.2 Bazel 构建

直接执行：

```bash
./build_bazel.sh
```

脚本支持：

- `-c / --clean`：清理
- `-t / --test`：编译并运行测试
- `-d / --debug`：debug 构建
- `-r / --rpm`：构建 RPM

例如：

```bash
./build_bazel.sh
./build_bazel.sh -t
./build_bazel.sh -d
```

### 7.3 直接使用 Bazel target

当前 `BUILD.bazel` 中主要 target：

- `//:os_transport_hdrs`
- `//:os_transport_prefixed_hdrs`
- `//:os_transport_lib`
- `//:libos_transport.so`
- `//:test_thread_pool`
- `//:test_os_transport_unit`

如果直接用 Bazel，可执行：

```bash
bazel build //:libos_transport.so
bazel test //:test_thread_pool //:test_os_transport_unit
```

---

## 8. 头文件导出方式

当前 `BUILD.bazel` 同时导出了两种头文件引用形式：

### 8.1 普通导出

```bazel
cc_library(
    name = "os_transport_hdrs",
    hdrs = glob(["include/*.h"]),
    includes = ["include"],
)
```

对应包含方式：

```c
#include "os_transport.h"
```

### 8.2 带前缀导出

```bazel
cc_library(
    name = "os_transport_prefixed_hdrs",
    hdrs = glob(["include/*.h"]),
    strip_include_prefix = "include",
    include_prefix = "os-transport",
)
```

对应包含方式：

```c
#include "os-transport/os_transport.h"
```

这个设计是为了方便在外部仓库中按更稳定的 include 前缀引用该头文件。

---

## 9. 外部依赖

当前仓库依赖：

- `pthread`
- `URMA`
- `CUDA Runtime`

### 9.1 CMake 侧

`CMakeLists.txt` 会自动查找：

- `liburma.so`
- `libcudart.so`

默认搜索路径包括：

- `/usr/lib64`
- `/usr/local/lib`
- `/usr/local/cuda/lib64`
- `/opt/urma/lib64`

### 9.2 Bazel 侧

`BUILD.bazel` 通过：

- `@local_cuda//:cuda`
- `@local_urma//:urma`

接入系统上的 CUDA/URMA。

对应规则定义在：

- `third_party/BUILD.cuda`
- `third_party/BUILD.urma`

通常需要在主工程的 `WORKSPACE` 中通过 `new_local_repository(...)` 实例化。

---

## 10. 测试与示例

当前仓库测试/示例文件包括：

- `test/test_thread_pool.c`：线程池功能测试
- `test/test_os_transport_unit.c`：`os_transport.c` 白盒单元测试
- `test/os_transport_sample.c`：基础示例
- `test/urma_sample.c`：URMA 相关示例

Bazel 测试：

```bash
bazel test //:test_thread_pool //:test_os_transport_unit
```

脚本测试：

```bash
./build_bazel.sh -t
```

---

## 11. 安装与产物

### 11.1 动态库

安装后主要产物为：

```text
/usr/lib64/libos_transport.so
/usr/lib64/libos_transport.so.1
/usr/lib64/libos_transport.so.1.0.0
```

### 11.2 头文件

对外安装头文件为：

```text
/usr/include/os-transport/os_transport.h
```

当前对外只安装 `os_transport.h`，其他内部头文件不作为公开开发接口。

---

## 12. 当前代码特征与注意事项

### 12.1 全局初始化状态

当前 `src/os_transport.c` 中使用了：

```c
static int g_inited = 0;
```

用于标记库是否已初始化。

### 12.2 失败后的清理策略

当前实现里，如果批量任务等待失败，会通过：

```c
thread_pool_cancel_tasks_by_req(...)
```

按 `request_id` 取消剩余未执行任务。

### 12.3 线程命名

你当前这版仓库里，`worker_routine()` 还没有显式设置线程名。如果后续需要在 `top -H`、`ps -T`、`/proc/<pid>/task/*/comm` 中区分 worker，需要额外调用 `pthread_setname_np()`。

### 12.4 超时机制

从当前仓库代码来看，`wait_and_free_sync()` 仍然是**无超时阻塞等待**，也就是：

- 只要 `request_completed` 没置位
- 就会一直等待

如果后续要复用上层超时机制，需要在上层调用链或这里继续扩展。

---

## 13. 建议的典型接入方式

如果这个仓库作为独立库被外部工程接入，推荐方式是：

1. 初始化 `os_transport` 句柄。
2. 注册或更新 JFC/JFCE。
3. 调用 `os_transport_send()` / `os_transport_recv()` 发起任务。
4. 在 URMA completion 回调里调用 `os_transport_wake_up_task()`。
5. 在请求结束阶段调用 `wait_and_free_sync()`。
6. 进程退出前调用 `os_transport_destroy()`。

---

## 14. 当前仓库适用场景

这个仓库当前更适合如下场景：

- 多分片 Host -> Host -> GPU 的流水线搬运
- 需要按请求维度控制任务推进顺序
- 需要按 `request_id` 定位和清理剩余任务
- 外部系统已经具备自己的超时、调度和通知机制

---


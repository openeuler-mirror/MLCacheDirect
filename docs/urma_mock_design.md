# URMA Mock 设计原理与实现机制

## 一、设计背景

URMA（User-space Remote Memory Access）是华为自研的用户态远程内存访问框架，依赖特定的硬件和 SDK 环境。为了在普通服务器（无 URMA 硬件/SDK）上进行开发、测试和 CI，项目设计了一套完整的 mock 方案。

**核心目标：**
- 在无 URMA SDK 的环境下编译通过
- 在无 URMA 硬件的环境下运行测试
- 保持与真实 URMA API 的 ABI 兼容
- 使用本地内存拷贝模拟远程内存访问

## 二、设计原理

### 2.1 分层架构

URMA mock 采用**三层架构**：

```
┌─────────────────────────────────────────────────────────────┐
│              os_transport Layer                             │
│   (os_transport.c, os_transport_urma.c, thread_pool.c)     │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Type Shim Layer (urma_abi_compat.h)            │
│    提供 URMA 类型定义、枚举、常量，替代真实 SDK 头文件         │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│              Mock Runtime Layer (urma_mock_backend.c)       │
│    使用内存拷贝实现 URMA 语义，模拟远程内存访问                │
└─────────────────────────────────────────────────────────────┘
```

### 2.2 编译时依赖消除

**关键设计：Type Shim（类型垫片）**

项目通过 `mock/urma_abi_compat.h` 提供一套**兼容的类型声明**，完全替代真实的 URMA SDK 头文件（`ub/umdk/urma/urma_api.h`）。

**类型处理策略分为三类：**

| 类型类别 | 处理方式 | 示例 |
|---------|---------|------|
| **不透明指针类型** | 前向声明，不定义内容 | `urma_jfce_t`, `urma_target_seg_t` |
| **部分透明类型** | 最小完整定义，仅包含访问的字段 | `urma_jetty_t`, `urma_jfc_t`, `urma_jfs_t` |
| **值传递类型** | 完整定义，与真实 SDK 对齐 | `urma_context_t`, `urma_device_t`, `urma_token_t` |

**核心代码示例（部分透明类型）：**

```c
struct urma_jetty {
    struct {
        uint32_t id;
    } jetty_id;
    uint32_t state;
    void *priv;
};
typedef struct urma_jetty urma_jetty_t;

struct urma_jfc {
    struct {
        uint32_t id;
    } jfc_id;
    uint32_t depth;
    void *priv;
};
typedef struct urma_jfc urma_jfc_t;
```

**前向声明示例（不透明类型）：**

```c
typedef struct urma_jfce urma_jfce_t;
typedef struct urma_target_jetty urma_target_jetty_t;
typedef struct urma_target_seg urma_target_seg_t;
```

**ABI 对齐保证：**

```c
typedef enum urma_status_t {
    URMA_SUCCESS = 0,
    URMA_ERROR = -1,
    URMA_E_FAIL = -1,
    URMA_E_NOT_SUPPORT = -2,
    // ...
} urma_status_t;

#define URMA_EID_SIZE 16
#define URMA_TOKEN_SIZE 64
#define URMA_MAX_JETTY_CNT 16
```

### 2.3 运行时条件编译

**关键设计：条件编译宏 `OS_TRANSPORT_MOCK_MODE`**

通过条件编译实现 mock 模式与真实模式的切换：

```c
#ifdef OS_TRANSPORT_MOCK_MODE
#include "urma_abi_compat.h"
#else
#include <ub/umdk/urma/urma_api.h>
#ifdef URMA_OVER_UB
#include <ub/umdk/urma/urma_ubagg.h>
#endif
#endif
```

**内存布局兼容保证：**

所有 URMA 相关数据结构定义与真实 SDK 保持一致，确保：
- 指针传递安全
- 字段偏移正确
- 联合类型内存布局兼容

## 三、实现机制

### 3.1 Mock 后端实现

Mock 后端通过 `mock/urma_mock_backend.c` 实现所有 URMA API 的模拟函数：

```c
urma_status_t urma_post_jfs_wr(urma_jfs_t *jfs, const urma_jfs_wr_t *wr,
                                const urma_jfs_wr_t **bad_wr)
{
    if (!wr) return URMA_E_INVALID_PARAM;
    if (wr->rw.src.tseg && wr->rw.dst.tseg) {
        memcpy((void *)(uintptr_t)wr->rw.dst.addr,
               (void *)(uintptr_t)wr->rw.src.addr,
               wr->rw.src.len);
    }
    return URMA_SUCCESS;
}

urma_status_t urma_post_jetty_send_wr(urma_jetty_t *jetty,
                                       const urma_jetty_wr_t *wr,
                                       const urma_jetty_wr_t **bad_wr)
{
    if (!wr) return URMA_E_INVALID_PARAM;
    if (wr->rw.src.tseg && wr->rw.dst.tseg) {
        memcpy((void *)(uintptr_t)wr->rw.dst.addr,
               (void *)(uintptr_t)wr->rw.src.addr,
               wr->rw.src.len);
    }
    return URMA_SUCCESS;
}
```

**核心 mock 函数列表：**

| Mock 函数 | 对应真实 URMA API | 实现逻辑 |
|-----------|-------------------|----------|
| `urma_post_jfs_wr` | JFS 写操作 | 直接 memcpy 模拟数据传输 |
| `urma_post_jetty_send_wr` | Jetty 发送操作 | 直接 memcpy 模拟数据传输 |
| `urma_post_jetty_recv_wr` | Jetty 接收操作 | 返回成功，模拟接收队列 |
| `urma_post_jfr_wr` | JFR 接收操作 | 返回成功，模拟接收队列 |
| `urma_init/create_context/create_jfc/create_jetty` | 资源创建 | 返回成功，不分配真实资源 |

### 3.2 编译配置控制

**CMake 层面的开关控制：**

```cmake
option(OS_TRANSPORT_MOCK_MODE "Use mock URMA implementation" OFF)

if(OS_TRANSPORT_MOCK_MODE)
    add_definitions(-DOS_TRANSPORT_MOCK_MODE=1)
    file(GLOB MOCK_SRC_FILES mock/*.c)
    list(APPEND SRC_FILES ${MOCK_SRC_FILES})
    message(STATUS "URMA mock mode enabled")
else()
    find_library(URMA_LIBRARY NAMES urma PATHS /usr/lib64)
    # ... URMA 库查找逻辑
endif()
```

**build.sh 参数映射：**

```bash
case "${OPT}" in
    mock)
        OS_TRANSPORT_MOCK_MODE="ON"
        echo "✅ 启用URMA mock模式"
        ;;
```

### 3.3 包含路径配置

**主库目标包含路径：**

```cmake
target_include_directories(os_transport
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/mock>
        $<INSTALL_INTERFACE:include/os-transport>
)
```

**测试目标包含路径：**

```cmake
target_include_directories(test_thread_pool
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        $<$<BOOL:${OS_TRANSPORT_MOCK_MODE}>:${CMAKE_CURRENT_SOURCE_DIR}/mock>
)
```

## 四、关键数据结构处理

### 4.1 Jetty 相关类型

| 类型 | 定义方式 | 访问字段 |
|-----|---------|---------|
| `urma_jetty_t` | 完整定义 | `.jetty_id.id` |
| `urma_jfs_t` | 完整定义 | `.jfs_id.id`, `.jetty_id` |
| `urma_target_jetty_t` | 完整定义 | `.jetty_id` |

### 4.2 Context 相关类型

| 类型 | 定义方式 | 访问字段 |
|-----|---------|---------|
| `urma_context_t` | 完整定义 | `.async_fd`, `.eid`, `.uasid` |
| `urma_device_t` | 完整定义 | `.type`, `.eid_cnt`, `.name` |

### 4.3 Segment 相关类型

| 类型 | 定义方式 | 访问字段 |
|-----|---------|---------|
| `urma_seg_t` | 完整定义 | `.ubva`, `.attr`, `.token_id` |
| `urma_target_seg_t` | 前向声明 | 仅指针传递 |
| `urma_seg_cfg_t` | 完整定义 | `.va`, `.len`, `.access` |

### 4.4 JFC/JFR 相关类型

| 类型 | 定义方式 | 访问字段 |
|-----|---------|---------|
| `urma_jfc_t` | 完整定义 | `.jfc_id.id`, `.depth` |
| `urma_jfr_t` | 完整定义 | `.jfr_id.id` |
| `urma_jfc_cfg_t` | 完整定义 | `.depth`, `.flag`, `.jfce` |

### 4.5 Work Request 相关类型

| 类型 | 定义方式 | 访问字段 |
|-----|---------|---------|
| `urma_jfs_wr_t` | 完整定义 | `.opcode`, `.flag`, `.tjetty`, `.user_ctx`, `.rw` |
| `urma_jetty_wr_t` | 完整定义 | `.opcode`, `.flag`, `.user_ctx`, `.rw` |
| `urma_jfr_wr_t` | 完整定义 | `.src`, `.user_ctx`, `.next` |
| `urma_sge_t` | 完整定义 | `.addr`, `.len`, `.tseg` |

### 4.6 Completion 相关类型

| 类型 | 定义方式 | 访问字段 |
|-----|---------|---------|
| `urma_cr_t` | 完整定义 | `.status`, `.completion_len`, `.byte_cnt`, `.local_id`, `.user_ctx`, `.opcode`, `.immediate_data`, `.imm_data` |

## 五、架构优势

### 5.1 无侵入性

- 原有代码逻辑未修改，通过条件编译切换
- `os_transport.h` 作为唯一入口，统一管理 URMA 头文件包含
- 项目其他文件无需感知 mock 模式的存在

### 5.2 ABI 兼容性

- 类型定义、枚举值、常量与真实 SDK 严格对齐
- 支持通过指针传递和访问关键字段
- 联合类型（如 `urma_cr_t`）内存布局与真实 SDK 兼容

### 5.3 灵活性

- Mock 后端使用内存拷贝实现，支持单机开发和测试
- 可独立于 URMA 硬件进行开发和 CI
- 测试线程池等组件无需依赖真实 URMA 环境

### 5.4 可扩展性

- 新的 URMA API 只需在 `urma_mock_backend.c` 中添加对应的 mock 实现
- 新的类型只需在 `urma_abi_compat.h` 中添加声明
- CMake 配置自动包含 mock 目录下的所有源文件

## 六、编译使用方式

```bash
# 正常模式（需要 URMA SDK）
./build.sh

# Mock 模式（不需要 URMA SDK）
./build.sh --mock

# Mock 模式 + 测试
./build.sh --mock -t

# Mock 模式 + 覆盖率
./build.sh --mock --gcov-ut

# Mock 模式 + 故障注入
./build.sh --mock --inject -t
```

**参数说明：**
- `--mock`: 启用 URMA mock 模式
- `-t`: 编译测试程序
- `--gcov-ut`: 启用覆盖率插桩
- `--inject`: 启用故障注入

## 七、文件结构

```
MLCacheDirect/
├── mock/
│   ├── urma_abi_compat.h      # URMA 类型垫片（核心文件）
│   └── urma_mock_backend.c    # URMA mock 后端实现
├── include/
│   ├── os_transport.h         # 条件编译包含 URMA 头文件
│   ├── os_transport_urma.h    # URMA 传输接口声明
│   └── os_transport_thread_pool_internal.h  # 移除直接 URMA 头文件依赖
├── src/
│   ├── os_transport.c         # 传输核心逻辑
│   ├── os_transport_urma.c    # URMA 传输实现
│   └── os_transport_thread_pool.c  # 线程池实现
├── CMakeLists.txt             # CMake 配置，添加 mock 模式支持
└── build.sh                   # 构建脚本，添加 --mock 参数
```

## 八、与 yuanrong-datasystem 的差异

| 对比项 | yuanrong-datasystem | MLCacheDirect |
|--------|---------------------|---------------|
| **架构** | 三层：Wrapper + Type Shim + Mock Backend | 两层：Type Shim + Mock Backend |
| **动态加载** | 使用 dlopen + weak symbol 实现运行时动态切换 | 使用条件编译实现编译时切换 |
| **模拟方式** | shm + UDS 模拟远程内存访问 | 直接 memcpy 模拟数据传输 |
| **适用场景** | 需要模拟分布式环境，支持多进程通信 | 本地开发测试，验证线程池和传输逻辑 |
| **复杂度** | 较高，支持完整的分布式模拟 | 较低，专注于编译和基本功能验证 |

**设计决策说明：**

MLCacheDirect 作为底层库，选择了更简单的条件编译方案：
1. **编译时切换**：避免运行时动态加载的复杂性
2. **直接内存拷贝**：满足本地开发测试需求，无需 shm+UDS 的开销
3. **两层架构**：去掉 wrapper 层，减少代码冗余

## 九、验证结果

### 9.1 编译验证

```
✅ URMA mock mode enabled
✅ 编译生成 libos_transport.so
✅ 打包生成 RPM 包
```

### 9.2 测试验证

```
Starting thread pool tests...
=== Test 1: Single tasks ===
Test 1 passed.
=== Test 2: Batch tasks ===
Test 2 passed.
...
All tests passed!
```

### 9.3 无侵入性验证

- ✅ 原有代码逻辑未修改
- ✅ mock 模式编译通过
- ✅ 正常模式编译通过（需要 URMA SDK）
- ✅ 测试用例行为一致

## 十、Known Limitations

### 10.1 测试用例编译问题

以下测试用例存在预编译失败问题，与 mock 模式无关：

| 测试目标 | 错误原因 | 状态 |
|---------|---------|------|
| `test_os_transport_log_unit` | `ost_log_write` 参数传递错误，实参数量不足 | 待修复 |
| `test_os_transport_unit` | `OS_TRANSPORT_MAX_RECV_LEN` 常量未定义 | 待修复 |

这些问题是测试代码本身的问题，需要单独修复。

### 10.2 Mock 功能限制

- **异步事件模拟**：mock 后端不模拟真实的 URMA completion 事件，`urma_poll` 始终返回 0，`urma_get_cr` 始终返回 `URMA_E_AGAIN`
- **远程内存访问模拟**：mock 使用本地内存拷贝模拟远程内存访问，不支持跨进程/跨节点通信
- **传输状态模拟**：mock 不维护真实的传输状态机，所有操作直接返回成功

### 10.3 适用场景

Mock 模式适用于：
- ✅ 本地开发和调试
- ✅ 线程池功能验证
- ✅ CI 环境编译测试
- ✅ 故障注入测试

Mock 模式不适用于：
- ❌ 真实远程内存访问验证
- ❌ 性能基准测试
- ❌ 跨节点通信测试
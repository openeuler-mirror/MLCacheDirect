# MLCacheDirect README

## 1. Introduction

`MLCacheDirect` is a transport library based on asynchronous pipelined chunking. Its core deliverables consist of:

- Dynamic library: `libos_transport.so`
- Public header: `include/os_transport.h`

Core responsibilities:

1. Splits large-scale data transfers into fixed-size chunks.
2. Submits `write with notify` or `recv` requests via URMA.
3. Utilizes a thread pool to organize downstream processing for the same batch of chunks based on `request_id`.
4. Wakes up the corresponding worker thread upon receiving a completion notification to drive subsequent tasks for the request.
5. Provides callers with synchronous semantics following the following workflow: submit a request, wait for batch completion, cancel remaining tasks on failure.

---

## 2. Directory Structure

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

Notes:

- Core library code in the root directory has no dependencies on the CUDA runtime.
- `tools/datasystem_test/pipeline_h2d.cpp` is an upper-level integration test case. It still utilizes `cudaMalloc/cudaMemcpy` to validate the new paradigm where the upper layer independently handles host-to-device (H2D) transfers.

---

## 3. Architecture and Responsibility Boundaries

### 3.1 What the Current Library Handles

The `MLCacheDirect` library is responsible for:

- Chunking
- URMA request submission
- Completion event unpacking
- Waking up thread pool workers based on `request_id`
- Executing the next chunk's transmission task on the sender side within the worker thread
- Executing `notify_callback` on the receiver side within the worker thread
- Request-level synchronization and resource recycling

### 3.2 What the Current Library No Longer Handles

The library is no longer directly responsible for CUDA runtime operations, such as:

- `cudaSetDevice`
- `cudaMalloc`
- `cudaMemcpy`
- `cudaEventRecord`
- CUDA stream/event lifecycle management

Note: These operations must now be handled entirely by the upper layers, including but not limited to:

- SDK
- datasystem client
- Application-specific GPU data transport modules

---

## 4. Core Data Structures

### 4.1 `os_transport_user_data_t`

Defined in `include/os_transport.h`:

```c
typedef union {
    struct {
        uint64_t request_id : 60;
        uint64_t chunk_type : 1;
        uint64_t chunk_id : 3;
    } bs;
    uint64_t user_ctx;
} os_transport_user_data_t;
```

This is the most critical completion context within the library, encoding the following information:

- `request_id`: unique identifier for the entire request batch. The API accepts a contiguous 40-bit ID. For
  WRITE_WITH_IMM, the library encodes it into bits `[59:40]` and `[19:0]` of the transport field and restores it
  when the completion is received.
- `chunk_id`: index of the current chunk.
- `chunk_type`: type of the current chunk.
  - `NOT_SPLIT`
  - `MIDDLE_CHUNK`
  - `LAST_CHUNK`

Send path: This information is packed into `notify_data`/`user_ctx` of URMA.
Receive path: `os_transport_wake_up_task()` parses this information from the completion event and passes it through to the upper-level callback.

### 4.2 `ost_buffer_info_t`

```c
typedef struct {
    uint64_t addr;
    urma_target_seg_t *tseg;
} ost_buffer_info_t;
```

Represents a host-side buffer:

- `addr`: buffer address.
- `tseg`: URMA target segment information.

### 4.3 `ost_device_info_t`

```c
typedef struct {
    urma_jfr_t *jfr;
    void *dst;
} ost_device_info_t;
```

Although the structure is still named `device_info`, in the current codebase, it only provides the following for the receive (`recv`) path:

- `jfr`: receiver-side resources used for submitting URMA recv requests.
- `dst`: target address.

Note: The library does not directly perform `cudaMemcpy` on `dst`. It is only used for chunk address calculation and to inform the upper layer in the subsequent callback chain of the target logical address range for this batch of data.

### 4.4 `notify_callback_t`

```c
typedef int (*notify_callback_t)(void *user_data);
```

This is the critical extension point for the current receive (`recv`) path.

The callback parameter `user_data` points to a copy of `os_transport_user_data_t`. This copy is populated by `os_transport_wake_up_task()` after parsing the completion event and before the worker executes.

This means that the upper layer can obtain the following within `notify_callback`:

- `request_id`
- `chunk_id`
- `chunk_type`

Based on this information, the upper layer can decide the subsequent actions, such as:

- Locating the host buffer or device pointer for the corresponding chunk
- Triggering `cudaMemcpyAsync`
- Handling the final notification for the last chunk.

---

## 5. Code Modules

### 5.1 `src/os_transport.c`

This is the main entry file of the library, responsible for:

- External API implementation
- Parameter validation
- Chunking
- Task group construction
- Thread pool registration
- Worker execution logic after completion
- Waiting for request completion and resource recycling

The two most critical types of worker tasks are:

#### send worker

`send_task_worker_func()` is responsible for calling `urma_write_with_notify()` to continue transmitting subsequent chunks.

#### recv worker

`recv_task_worker_func()` does not perform CUDA copies. Instead, it:

1. Extracts completion metadata from `recv_task_arg->notify_user_data`.
2. Invokes the upper-level registered `notify_callback(&recv_task_arg->notify_user_data)`.
3. Determines whether the current chunk succeeded based on the callback return value.
4. Updates the completion count for the entire batch of tasks.

This is the core implementation point after moving `cudaMemcpy` out of the library.

### 5.2 `src/os_transport_thread_pool.c`

This is the thread pool implementation, responsible for:

- Worker creation and destruction
- Task queue management
- Pending request queue management
- `request_id -> worker` binding
- Waking up workers by `request_id` upon receiving completions
- Batch task submission
- Canceling unexecuted tasks by request

Key points of the current design:

- Tasks with the same `request_id` are bound to the same worker.
- The actual execution timing of a task by a worker is driven and advanced by completion wake-ups.
- An entire batch of tasks shares a single `task_sync_t`, allowing the main thread to perform a unified wait.

### 5.3 `src/os_transport_urma.c`

Responsible for URMA encapsulation:

- `urma_write_chunk()`
- `urma_write_notify()`
- `urma_recv_with_notify()`

Send path: Transmits data via `URMA_OPC_WRITE_IMM`, carrying `notify_data`/`user_ctx`.
Receive path: Responsible for posting receive work requests to `jfr`.

### 5.4 `src/os_transport_log.c`

Responsible for log registration and log output.

External API:

```c
int os_transport_log_reg(int level, log_callback_t cb);
```

---

## 6. Chunking Mechanism

The default chunk size is defined in `include/os_transport.h`:

```c
#define DEFAULT_CHUNK_SIZE (2 * 1024 * 1024)
```

The default is **2 MB**.

### 6.1 Common Chunking Rules for Send/Recv

- When `len <= DEFAULT_CHUNK_SIZE`, it is processed as a single chunk.
- When `len > DEFAULT_CHUNK_SIZE`, it is split into multiple chunks.
- Each chunk is assigned its own `chunk_id` and `chunk_type`, with at most 8 chunks supported.

### 6.2 Send Path Chunking Characteristics

When the data size is greater than 2 MB:

1. The main thread registers the entire batch of send tasks to the thread pool.
2. The first chunk is transmitted manually and immediately.
3. Subsequent chunks are transmitted sequentially by workers, driven by completion events.
4. The caller waits for the completion of the entire batch via `wait_and_free_sync()`.

### 6.3 Recv Path Chunking Characteristics

When the data size is greater than 2 MB:

1. The main thread slices all chunks first.
2. A receive task is registered for each chunk.
3. `urma_recv_with_notify()` is called to post a receive request for each chunk.
4. Upon completion arrival, the thread pool wakes up the corresponding worker by `request_id`.
5. The worker invokes the upper-level `notify_callback`.
6. The caller waits for the completion of the entire batch via `wait_and_free_sync()`.

Note:

> In the receive path, completion semantics currently imply that:
> 
> - The URMA completion event has arrived.
> - The corresponding `notify_callback` has finished execution and returned.
>
> It does not automatically guarantee that "GPU data is ready" unless your `notify_callback` explicitly implements and handles that synchronization.

---

## 7. External API Description

### 7.1 Initialization

```c
uint32_t os_transport_init(urma_context_t *urma_ctx,
                           os_transport_cfg_t *ost_cfg,
                           void **handle);
```

Purposes:

- Initializes `os_transport_handle_t`.
- Saves URMA context and configurations.
- Creates the thread pool.
- Registers JFC/JFCE.
- Starts the thread pool.

Key configurations:

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

### 7.2 Log Registration

```c
int os_transport_log_reg(int level, log_callback_t cb);
```

Registers the log callback.

### 7.3 Send API

```c
uint32_t os_transport_send(void *handle,
                           urma_jetty_info_t *jetty_info,
                           ost_buffer_info_t *local_src,
                           ost_buffer_info_t *remote_dst,
                           uint32_t len,
                           uint64_t server_key,
                           uint64_t client_key,
                           task_sync_t **ret_sync_handle,
                           urma_status_t *urma_status);
```

Purposes:

- Chunks host-to-host transmission data.
- Generates asynchronous send tasks.
- Transmits the first chunk immediately.
- Drives subsequent chunk transmissions through workers triggered by notifications.
- Returns a `task_sync_t` handle for the caller to wait for the entire batch to complete.

Description:

- `server_key` and `client_key` must be between `0` and `(1ULL << 40) - 1`; the API fails for values outside this
  range.
- The library encodes `client_key` before writing it to the completion pass-through field. The receive side
  restores it before using it as `request_id`.
- `server_key` is used for local completion or context differentiation.
- `urma_status` is optional. If a single-chunk write or the first chunk of a split write fails in
  `urma_write_with_notify()`, the API returns non-zero and reports the original URMA status through this parameter.
  For other library-side failures, the parameter remains `URMA_SUCCESS`.
- After the first chunk of a split write succeeds, statuses from subsequent asynchronous writes are reported by
  `wait_and_free_sync()`.

### 7.4 Recv API

```c
uint32_t os_transport_recv(void *handle,
                           ost_buffer_info_t *host_src,
                           ost_device_info_t *device_dst,
                           uint32_t len,
                           uint64_t client_key,
                           task_sync_t **ret_sync_handle,
                           notify_callback_t notify_callback);
```

Purposes:

- Chunks the receive data range.
- Registers a receive task for each chunk.
- Posts receive requests to the URMA `jfr`.
- Invokes the upper-level`notify_callback` upon completion arrival.
- Returns a `task_sync_t` handle for the caller to wait for the entire batch to complete.

`client_key` must be between `0` and `(1ULL << 40) - 1`. The callback receives the restored contiguous 40-bit
`request_id`.

Difference between this API and the legacy version:

> `notify_callback` is now **mandatory**.
> The library itself no longer performs `cudaMemcpy` inside the receive worker. Instead, it delegates the control of "how to process this chunk of data upon receiving the completion" entirely to the upper layer.

### 7.5 Completion Wake-up API

```c
int os_transport_wake_up_task(void *handle, void *cr_t);
```

Purposes:

- Parses `user_data` from the URMA completion event.
- Extracts `request_id`.
- Notifies the thread pool to wake up the corresponding worker.
- Forwards the `user_data` from this completion as pass-through information to the subsequent receive worker.

### 7.6 Waiting and Freeing Synchronization Resources

```c
uint32_t wait_and_free_sync(void *handle,
                            task_sync_t *sync_handle,
                            urma_status_t *urma_status);
```

Purposes:

- Blocks to wait for the entire batch of tasks for the current request to complete.
- Cancels remaining unexecuted tasks by `request_id` if a failure or incomplete completion is detected midway.
- Frees the synchronization objects, chunk arrays, and task group resources.
- `urma_status` is optional. For a send request, it reports the first non-success URMA status from an asynchronous
  write task. A non-zero wait result with `URMA_SUCCESS` indicates a timeout, cancellation, or library-side failure;
  a non-success URMA status indicates a write-task failure.

### 7.7 Cancelling Tasks for a Specific Request

```c
uint32_t os_transport_cancel_tasks(void *handle, task_sync_t **sync_handle, uint64_t request_id);
```

Purposes:

Cancels all unexecuted tasks associated with the specified `request_id`.

### 7.8 Destroying a Handle

```c
uint32_t os_transport_destroy(void *handle);
```

Purposes:

- Destroys the thread pool.
- Releases internal resources,

---

## 8. Typical Sequences

### 8.1 Send Path Sequence

```text
Caller
  -> os_transport_send()
      -> Slice chunks.
      -> Register subsequent tasks to the pool.
      -> Send the first chunk.

Remote/Local completion arrival
  -> os_transport_wake_up_task()
      -> Wake up the worker by request_id.
          -> The worker executes the next send task.
              -> urma_write_notify()
              -> urma_write_chunk()

Caller
  -> wait_and_free_sync()
      -> Wait the entire batch to complete.
```

### 8.2 Recv Path Sequence

```text
Caller
  -> os_transport_recv(..., notify_callback)
      -> Slice chunks.
      -> Register a recv task.
      -> Post receive requests via urma_recv_with_notify().

URMA completion arrival
  -> os_transport_wake_up_task()
      -> Parse request_id/chunk_id/chunk_type.
      -> Wake up the worker by request_id.
          -> The worker executes the recv task.
              -> notify_callback(&os_transport_user_data_t_copy)

Upper-layer notify_callback
  -> Match context by chunk_id.
  -> Trigger handling, such as cudaMemcpy/cudaMemcpyAsync.

Caller
  -> wait_and_free_sync()
      -> Wait the entire batch of recv tasks to complete.
```

---

## 9. How to Implement the New H2D Mode at the Upper Layer

Since `cudaMemcpy` has been removed from the library internal execution, it is recommended that the upper layer integrate the actual H2D logic into `notify_callback` or its subsequent scheduling pipeline.

Below is a minimalist pseudocode implementation demonstrating this approach:

```c
static int my_notify_callback(void *user_data)
{
    os_transport_user_data_t *ud = (os_transport_user_data_t *)user_data;
    if (!ud) {
        return -1;
    }

    uint64_t request_id = ud->bs.request_id;
    uint32_t chunk_id = ud->bs.chunk_id;
    uint32_t chunk_type = ud->bs.chunk_type;

    // 1. Find the upper-layer context saved for this request based on request_id.
    // 2. Find the corresponding host and device addresses based on chunk_id.
    // 3. Execute cudaMemcpyAsync(...) or perform other processing.
    // 4. If needed, perform additional cleanup or post-processing on the LAST_CHUNK.

    (void)chunk_type;
    return 0;
}
```

It is recommended that the upper layer maintains its own request context table, which should at least contain the following:

- `request_id`
- Original host base address
- Original device base address
- Offset for each chunk
- `stream`/`event`
- Whether it is the last chunk
- Whether a consolidated completion notification is required

This ensures that `notify_callback` can accurately map the completion metadata to actual GPU copy operations.

---

## 10. Build Instructions

### 10.1 CMake Build

The project root directory provides a `build.sh` scriptL

```bash
chmod +x build.sh
./build.sh
```

Run unit tests only:

```bash
./build.sh -t
```

Features of  `build.sh`:

- Automatically detects architecture (`x86_64/aarch64`).
- Sets the build directory as `build-<arch>`.
- Cleans up legacy CMake and Bazel build artifacts by default.
- Generates an RPM package by default.

Dependencies:

- `cmake`
- `gcc`
- `make`
- `rpmbuild` (Required during packaging)
- `liburma.so`

### 10.2 Bazel Build

The project root directory provides a `build_bazel.sh` script:

```bash
chmod +x build_bazel.sh
./build_bazel.sh
```

Run tests:

```bash
./build_bazel.sh -t
```

Clean build cache:

```bash
./build_bazel.sh -c
```

Specify the URMA path via environment variables:

```bash
URMA_INCLUDE_DIR=/usr/include \
URMA_LIB_DIR=/usr/lib64 \
./build_bazel.sh
```

### 10.3 Current Library Build Dependencies

The library currently depends only on:

- URMA
- pthread

It **does not depend on the CUDA runtime**.

This is directly evident from the current codebase:

- The `src/*.c` files in the root directory do not include `cuda_runtime.h`.
- `CMakeLists.txt` does not link against `CUDA::cudart`.
- `BUILD.bazel` does not introduce any CUDA dependencies.

Only the upper-layer joint integration test case, `tools/datasystem_test/pipeline_h2d.cpp`, still requires CUDA.

---

## 11. RPM Artifacts

The default packaging process generates the following:

- Main package: `os-transport-<version>-<release>.<arch>.rpm`
- Development package:`os-transport-devel-<version>-<release>.<arch>.rpm`

Installed artifacts:

- `libos_transport.so`
- `include/os_transport.h`

Currently, only the public header is exposed and installed:

- `include/os_transport.h`

Internal header files are not exported.

---

## 12. Testing

### 12.1 Unit Tests

The current repository includes two built-in tests:

- `test_thread_pool`
- `test_os_transport_unit`

Execution:

```bash
./build.sh -t
```

Or

```bash
./build_bazel.sh -t
```

### 12.2 Upper-Layer Joint Integration Tool

`tools/datasystem_test/pipeline_h2d.cpp` serves as a joint integration and validation program.

Purposes:

- Demonstrates how the upper layer allocates its own device memory.
- Demonstrates how the upper layer handles H2D operations within its business logic layer.
- Validate the new division of responsibilities: "The library strictly handles transport and notifications, while the upper layer handles CUDA memory copies."

For compilation and usage instructions regarding this tool, refer to:

- `tools/datasystem_test/README.md`

---

## 13. Usage Recommendations and Precautions

### 13.1 The `recv` Path Must Provide a `notify_callback`

The input validation for `os_transport_recv()` requires `notify_callback != NULL`.

In the current version, the primary responsibility of the recv worker is to execute this callback.

### 13.2 `wait_and_free_sync()` Only Guarantees Library-side Task Completion

The current wait semantics guarantee that:

- URMA task submission and completion advancement are finished.
- The corresponding task in the thread pool has completed.
- The upper-layer `notify_callback` has returned.

It does not inherently guarantee that:

- GPU kernels have finished executing.
- CUDA streams have been synchronized.
- Device data is immediately ready for consumption.

These guarantees must be supplemented by your `notify_callback` and its external synchronization logic.

### 13.3 `request_id` Is the Crucial Key for Whole-Batch Requests

Whether for `send` or `recv`, the `request_id` serves as the core index for the entire task orchestration. The upper layer should ideally ensure:

- `request_id` is unique among concurrently active requests.
- The business context can be quickly looked up using `request_id`.

### 13.4 If Callbacks Involve Heavy Workloads, the Upper Layer Should Manage Concurrency Independently

`notify_callback` currently runs within the thread pool's worker threads. If you execute time-consuming H2D transfers, synchronous waits, or complex business logic directly inside the callback, it will block that worker thread.

Therefore, the recommended approach is usually to:

- Perform only lightweight dispatching inside `notify_callback`.
- Delegate the actual CUDA operations to the upper layer's own thread or stream scheduling modules.

---

## 14. Summary

The current version of MLCacheDirect has transitioned from an internal "library-managed URMA + CUDA memcpy" model to a decoupled "library-managed URMA sliced transport + completion wakeup + upper-layer callback orchestration, with the upper layer taking full responsibility for H2D execution" model.

Consequently, when integrating this version, the priority is no longer searching for `cudaMemcpy` inside the library, but rather establishing these three key components at the upper layer:

1. `request_id -> business context` mapping
2. `chunk_id -> host/device offset` mapping
3. H2D and completion synchronization policy within `notify_callback`

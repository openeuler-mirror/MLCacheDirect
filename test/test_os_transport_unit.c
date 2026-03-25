#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/*
 * White-box unit tests for src/os_transport.c.
 * We include the .c file directly to access static helpers and isolate
 * external dependencies via local mocks below.
 */
#include "../src/os_transport.c"

/* Thread-pool mock controls and observations. */
static int g_mock_pool_init_fail = 0;
static int g_mock_pool_start_fail = 0;
static int g_mock_submit_fail = 0;
static uint32_t g_mock_destroy_calls = 0;
static uint32_t g_mock_last_submit_task_count = 0;
static uint32_t g_mock_last_submit_request_id = 0;
static void *g_mock_last_submit_task_arg0 = NULL;
static int g_mock_cancel_ret = 0;
static uint32_t g_mock_cancel_calls = 0;
static uint32_t g_mock_cancel_last_request_id = 0;

/* URMA write mock controls and observations. */
static urma_status_t g_mock_urma_write_status = URMA_SUCCESS;
static uint32_t g_mock_urma_write_calls = 0;
static urma_write_info_t g_mock_last_write_info;
static struct chunk_info g_mock_last_write_chunk;

/* CUDA memcpy mock controls and observations. */
static int g_mock_cuda_memcpy_ret = 0;
static uint32_t g_mock_cuda_memcpy_calls = 0;
static void *g_mock_cuda_last_dst = NULL;
static const void *g_mock_cuda_last_src = NULL;
static size_t g_mock_cuda_last_count = 0;
static enum cudaMemcpyKind g_mock_cuda_last_kind = cudaMemcpyHostToHost;
static cudaStream_t g_mock_cuda_last_stream = {0};

/* Reset all global mock states between tests. */
static void reset_mocks(void)
{
    g_mock_pool_init_fail = 0;
    g_mock_pool_start_fail = 0;
    g_mock_submit_fail = 0;
    g_mock_destroy_calls = 0;
    g_mock_last_submit_task_count = 0;
    g_mock_last_submit_request_id = 0;
    g_mock_last_submit_task_arg0 = NULL;
    g_mock_cancel_ret = 0;
    g_mock_cancel_calls = 0;
    g_mock_cancel_last_request_id = 0;

    g_mock_urma_write_status = URMA_SUCCESS;
    g_mock_urma_write_calls = 0;
    memset(&g_mock_last_write_info, 0, sizeof(g_mock_last_write_info));
    memset(&g_mock_last_write_chunk, 0, sizeof(g_mock_last_write_chunk));

    g_mock_cuda_memcpy_ret = 0;
    g_mock_cuda_memcpy_calls = 0;
    g_mock_cuda_last_dst = NULL;
    g_mock_cuda_last_src = NULL;
    g_mock_cuda_last_count = 0;
    g_mock_cuda_last_kind = cudaMemcpyHostToHost;
    memset(&g_mock_cuda_last_stream, 0, sizeof(g_mock_cuda_last_stream));
}

ThreadPoolHandle thread_pool_init(uint32_t worker_queue_cap, uint32_t pending_queue_cap)
{
    (void)worker_queue_cap;
    (void)pending_queue_cap;
    if (g_mock_pool_init_fail) {
        return NULL;
    }
    return calloc(1, sizeof(struct _ThreadPool));
}

int thread_pool_start(ThreadPoolHandle handle)
{
    if (!handle || g_mock_pool_start_fail) {
        return -1;
    }
    return 0;
}

uint64_t thread_pool_submit_task(ThreadPoolHandle handle, uint32_t request_id,
                                 int (*task_func)(void *arg), void *task_arg,
                                 TaskCompleteCb complete_cb, void *user_data)
{
    (void)handle;
    (void)request_id;
    (void)task_func;
    (void)task_arg;
    (void)complete_cb;
    (void)user_data;
    return 1;
}

uint64_t *thread_pool_submit_batch_tasks(ThreadPoolHandle handle, ThreadPoolTask *tasks,
                                         uint32_t task_count, TaskCompleteCb complete_cb,
                                         void *user_data, TaskCompleteCb batch_complete_cb,
                                         void *batch_user_data)
{
    (void)complete_cb;
    (void)user_data;
    (void)batch_complete_cb;
    (void)batch_user_data;

    if (!handle || !tasks || task_count == 0 || g_mock_submit_fail) {
        return NULL;
    }

    g_mock_last_submit_task_count = task_count;
    g_mock_last_submit_request_id = tasks[0].request_id;
    g_mock_last_submit_task_arg0 = tasks[0].task_arg;

    uint64_t *task_ids = calloc(task_count, sizeof(uint64_t));
    if (!task_ids) {
        return NULL;
    }
    for (uint32_t i = 0; i < task_count; i++) {
        task_ids[i] = i + 1;
    }
    return task_ids;
}

int async_poll_notify(ThreadPoolHandle handle, uint32_t notify_type, void *data)
{
    (void)handle;
    (void)notify_type;
    (void)data;
    return 0;
}

int thread_pool_cancel_tasks_by_req(ThreadPoolHandle handle, uint32_t request_id)
{
    if (!handle) {
        return -1;
    }
    g_mock_cancel_calls++;
    g_mock_cancel_last_request_id = request_id;
    return g_mock_cancel_ret;
}

void thread_pool_destroy(ThreadPoolHandle handle)
{
    g_mock_destroy_calls++;
    free(handle);
}

urma_status_t urma_write_with_notify(urma_write_info_t write_info, struct chunk_info *chunk_info)
{
    g_mock_urma_write_calls++;
    g_mock_last_write_info = write_info;
    if (chunk_info) {
        g_mock_last_write_chunk = *chunk_info;
    }
    return g_mock_urma_write_status;
}

int cudaMemcpyAsync(void *dst, const void *src, size_t count, enum cudaMemcpyKind kind,
                    cudaStream_t stream)
{
    g_mock_cuda_memcpy_calls++;
    g_mock_cuda_last_dst = dst;
    g_mock_cuda_last_src = src;
    g_mock_cuda_last_count = count;
    g_mock_cuda_last_kind = kind;
    g_mock_cuda_last_stream = stream;
    return g_mock_cuda_memcpy_ret;
}

static int dummy_task_func(void *arg)
{
    (void)arg;
    return 0;
}

/* Build a ready-to-use sync object with an allocated task group. */
static task_sync_t *create_sync_with_tasks(uint32_t task_num, int request_completed)
{
    task_sync_t *sync = NULL;
    task_group_t *task_group = NULL;

    assert(init_task_sync(&sync) == 0);
    assert(alloc_task_group(&task_group, task_num, sizeof(send_task_arg_t)) == 0);

    sync->task_group = task_group;
    sync->request_completed = request_completed;
    sync->total_tasks = task_num;
    return sync;
}

/* Build valid arguments for send-path API tests. */
static void build_valid_send_args(os_transport_handle_t *ost_handle, urma_jetty_info_t *jetty_info,
                                  struct buffer_info *local_src, struct buffer_info *remote_dst)
{
    memset(ost_handle, 0, sizeof(*ost_handle));
    memset(jetty_info, 0, sizeof(*jetty_info));
    memset(local_src, 0, sizeof(*local_src));
    memset(remote_dst, 0, sizeof(*remote_dst));

    ost_handle->thread_pool = (ThreadPoolHandle)0x1234;
    ost_handle->urma_event_mode = true;

    jetty_info->jfs = (urma_jfs_t *)0x11;
    jetty_info->jetty = (urma_jetty_t *)0x22;
    jetty_info->tjetty = (urma_target_jetty_t *)0x33;

    local_src->addr = 0x1000;
    local_src->tseg = (urma_target_seg_t *)0x44;

    remote_dst->addr = 0x2000;
    remote_dst->tseg = (urma_target_seg_t *)0x55;
}

/*
 * Resource helper tests.
 * Given invalid and valid allocations, verify task-group fields and ownership.
 */
static void test_alloc_task_group(void)
{
    task_group_t *task_group = NULL;

    assert(alloc_task_group(NULL, 1, sizeof(send_task_arg_t)) == -1);
    assert(alloc_task_group(&task_group, 0, sizeof(send_task_arg_t)) == -1);
    assert(alloc_task_group(&task_group, 1, 0) == -1);

    assert(alloc_task_group(&task_group, 3, sizeof(send_task_arg_t)) == 0);
    assert(task_group != NULL);
    assert(task_group->tasks != NULL);
    assert(task_group->task_args != NULL);
    assert(task_group->task_num == 3);

    free(task_group->task_args);
    free(task_group->tasks);
    free(task_group);
}

/*
 * Resource helper tests.
 * Given sync and group resources, verify helper free routines are null-safe.
 */
static void test_init_task_sync_and_free_helpers(void)
{
    task_sync_t *sync = NULL;
    task_group_t *task_group = NULL;

    assert(init_task_sync(NULL) == -1);
    assert(init_task_sync(&sync) == 0);
    assert(sync != NULL);

    assert(alloc_task_group(&task_group, 1, sizeof(send_task_arg_t)) == 0);
    sync->task_group = task_group;

    free_task_group_resource(NULL);
    free_task_group_resource(sync);
    assert(sync->task_group == NULL);

    free_sync_owned_resources(NULL);

    sync->chunks = malloc(sizeof(struct chunk_info));
    free_sync_owned_resources(sync);
}

/*
 * Synchronization helper tests.
 * Verify completion waiting behavior and completion-mark transition.
 */
static void test_wait_for_task_complete_and_mark(void)
{
    task_sync_t *sync = NULL;

    assert(wait_for_task_complete(NULL) == (uint32_t)-1);

    sync = create_sync_with_tasks(2, 1);
    sync->task_group->tasks[0].is_completed = true;
    sync->task_group->tasks[1].is_completed = false;
    assert(wait_for_task_complete(sync) == (uint32_t)-1);
    free_sync_owned_resources(sync);

    sync = create_sync_with_tasks(2, 0);
    sync->total_tasks = 2;
    sync->completed_tasks = 0;
    mark_task_group_completed(sync, true);
    assert(sync->completed_tasks == 1);
    assert(sync->request_completed == 0);
    mark_task_group_completed(sync, true);
    assert(sync->completed_tasks == 2);
    assert(sync->request_completed == 1);

    sync->task_group->tasks[0].is_completed = true;
    sync->task_group->tasks[1].is_completed = true;
    assert(wait_for_task_complete(sync) == 0);

    sync->request_completed = 0;
    sync->completed_tasks = 0;
    mark_task_group_completed(sync, false);
    assert(sync->request_completed == 1);
    assert(sync->completed_tasks == 0);

    mark_task_group_completed(NULL, true);
    free_sync_owned_resources(sync);
}

/*
 * Validation and mapping tests.
 * Verify update_jfc_for_poll, input validators, and build_write_info mapping.
 */
static void test_update_and_validate_and_build(void)
{
    struct _ThreadPool pool = {0};
    os_transport_handle_t handle = {0};
    urma_jetty_info_t jetty_info = {0};
    struct buffer_info local_src = {0};
    struct buffer_info remote_dst = {0};
    ost_device_info_t device_dst = {0};
    urma_write_info_t write_info;
    task_sync_t *sync_handle = NULL;

    assert(update_jfc_for_poll((urma_jfce_t *)0x1, (urma_jfc_t *)0x2, true, &pool) == 0);
    assert(pool.urmaInfo.jfce == (urma_jfce_t *)0x1);
    assert(pool.urmaInfo.jfc == (urma_jfc_t *)0x2);
    assert(pool.urmaInfo.urma_event_mode == true);

    handle.thread_pool = &pool;

    g_inited = 0;
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 1, &sync_handle) ==
           -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, &sync_handle) == -1);

    g_inited = 1;
    assert(validate_send_input(NULL, &jetty_info, &local_src, &remote_dst, 1, &sync_handle) ==
           -1);
    assert(validate_send_input(&handle, NULL, &local_src, &remote_dst, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, NULL, &remote_dst, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, &local_src, NULL, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 0, &sync_handle) ==
           -1);
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 1, NULL) == -1);

    assert(validate_recv_input(NULL, &local_src, &device_dst, 1, &sync_handle) == -1);
    assert(validate_recv_input(&handle, NULL, &device_dst, 1, &sync_handle) == -1);
    assert(validate_recv_input(&handle, &local_src, NULL, 1, &sync_handle) == -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 0, &sync_handle) == -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, NULL) == -1);

    jetty_info.jfs = (urma_jfs_t *)0x10;
    jetty_info.jetty = (urma_jetty_t *)0x20;
    jetty_info.tjetty = (urma_target_jetty_t *)0x30;
    local_src.tseg = (urma_target_seg_t *)0x40;
    remote_dst.tseg = (urma_target_seg_t *)0x50;

    write_info = build_write_info(&jetty_info, &local_src, &remote_dst, 123, 456);
    assert(write_info.jfs == jetty_info.jfs);
    assert(write_info.jetty == jetty_info.jetty);
    assert(write_info.target_jfr == jetty_info.tjetty);
    assert(write_info.src_tseg == local_src.tseg);
    assert(write_info.dst_tseg == remote_dst.tseg);
    assert(write_info.flag.value == 0);
    assert(write_info.user_ctx_server == 123);
    assert(write_info.user_ctx_client == 456);
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 1, &sync_handle) ==
           0);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, &sync_handle) == 0);

    g_inited = 0;
}

/*
 * Chunk split tests.
 * Verify split counts, address arithmetic, and invalid-argument guards.
 */
static void test_split_chunk_functions(void)
{
    struct chunk_info *chunks = NULL;
    uint64_t chunk_num = 0;
    struct buffer_info local = {0};
    struct buffer_info remote = {0};
    struct buffer_info host = {0};
    ost_device_info_t device = {0};

    assert(common_split_chunks(0x1000, 0x2000, DEFAULT_CHUNK_SIZE * 2 + 5, &chunks, &chunk_num) == 0);
    assert(chunks != NULL);
    assert(chunk_num == 3);
    assert(chunks[0].src == 0x1000);
    assert(chunks[0].dst == 0x2000);
    assert(chunks[0].len == DEFAULT_CHUNK_SIZE);
    assert(chunks[1].src == 0x1000 + DEFAULT_CHUNK_SIZE);
    assert(chunks[1].dst == 0x2000 + DEFAULT_CHUNK_SIZE);
    assert(chunks[1].len == DEFAULT_CHUNK_SIZE);
    assert(chunks[2].src == 0x1000 + DEFAULT_CHUNK_SIZE * 2);
    assert(chunks[2].dst == 0x2000 + DEFAULT_CHUNK_SIZE * 2);
    assert(chunks[2].len == 5);
    free(chunks);

    assert(send_split_chunks(NULL, &remote, 1, &chunks, &chunk_num) == (uint32_t)-1);
    assert(send_split_chunks(&local, NULL, 1, &chunks, &chunk_num) == (uint32_t)-1);
    assert(send_split_chunks(&local, &remote, 0, &chunks, &chunk_num) == (uint32_t)-1);

    local.addr = 0x3000;
    remote.addr = 0x8000;
    assert(send_split_chunks(&local, &remote, DEFAULT_CHUNK_SIZE + 1, &chunks, &chunk_num) == 0);
    assert(chunk_num == 2);
    assert(chunks[0].src == 0x3000);
    assert(chunks[0].dst == 0x8000);
    assert(chunks[1].len == 1);
    free(chunks);

    assert(recv_split_chunks(NULL, &device, 1, &chunks, &chunk_num) == (uint32_t)-1);
    assert(recv_split_chunks(&host, NULL, 1, &chunks, &chunk_num) == (uint32_t)-1);
    assert(recv_split_chunks(&host, &device, 0, &chunks, &chunk_num) == (uint32_t)-1);

    host.addr = 0x9000;
    device.dst = (void *)0xA000;
    assert(recv_split_chunks(&host, &device, DEFAULT_CHUNK_SIZE + 1, &chunks, &chunk_num) == 0);
    assert(chunk_num == 2);
    assert(chunks[0].src == 0x9000);
    assert(chunks[0].dst == 0xA000);
    assert(chunks[1].len == 1);
    free(chunks);
}

/*
 * Task-argument construction tests.
 * Verify user_ctx bitfields, recv arg initialization, and worker-task construction.
 */
static void test_construct_and_worker_helper_functions(void)
{
    urma_write_info_t write_info = {0};
    urma_recv_info_t recv_info = {0};
    struct chunk_info chunk = {.src = 0x1, .dst = 0x2, .len = 123};
    task_sync_t *sync = NULL;
    send_task_arg_t send_arg = {0};
    recv_task_arg_t recv_arg;
    ThreadPoolTask worker_task;
    os_transport_user_data_t user_server;
    os_transport_user_data_t user_client;

    assert(init_task_sync(&sync) == 0);

    write_info.user_ctx_server = 77;
    write_info.user_ctx_client = 99;
    construct_send_task_arg(&send_arg, write_info, &chunk, 5, false, sync);
    user_server.user_ctx = send_arg.write_info.user_ctx_server;
    user_client.user_ctx = send_arg.write_info.user_ctx_client;
    /*
     * user_ctx_server/client are stored as uint32_t in urma_write_info_t,
     * so we assert chunk metadata encoding here (stable across platforms).
     */
    assert(user_server.bs.chunk_type == MIDDLE_CHUNK);
    assert(user_server.bs.chunk_id == 5);
    assert(user_server.bs.chunk_size == 123);
    assert(user_client.bs.chunk_type == MIDDLE_CHUNK);

    construct_send_task_arg(&send_arg, write_info, &chunk, 6, true, sync);
    user_server.user_ctx = send_arg.write_info.user_ctx_server;
    assert(user_server.bs.chunk_type == LAST_CHUNK);
    assert(user_server.bs.chunk_id == 6);

    memset(&recv_arg, 0xAB, sizeof(recv_arg));
    recv_info.request_id = 55;
    recv_info.device_info.dst = (void *)0x12345;
    construct_recv_task_arg(&recv_arg, recv_info, &chunk, true, sync);
    assert(recv_arg.recv_info.request_id == 55);
    assert(recv_arg.chunk_info == &chunk);
    assert(recv_arg.is_last_chunk == true);
    assert(recv_arg.sync == sync);

    worker_task = construct_worker_task(100, 200, dummy_task_func, &chunk);
    assert(worker_task.task_id == 100);
    assert(worker_task.request_id == 200);
    assert(worker_task.task_func == dummy_task_func);
    assert(worker_task.task_arg == &chunk);
    assert(worker_task.is_completed == false);
    assert(worker_task.free_task_self == false);

    free_sync_owned_resources(sync);
}

/*
 * Worker wrapper tests.
 * Verify send/recv worker wrappers forward return codes and mark completion.
 */
static void test_do_chunk_and_worker_funcs(void)
{
    task_sync_t *sync_send = NULL;
    task_sync_t *sync_recv = NULL;
    struct chunk_info chunk = {.src = 0x1, .dst = 0x2, .len = 8};
    urma_write_info_t write_info = {0};
    urma_recv_info_t recv_info = {0};
    send_task_arg_t send_arg = {0};
    recv_task_arg_t recv_arg = {0};

    reset_mocks();
    g_mock_urma_write_status = 7;
    assert(do_send_chunk_for_worker(write_info, &chunk) == 7);
    assert(g_mock_urma_write_calls == 1);
    recv_info.device_info.stream.i = 9;
    assert(do_recv_chunk_for_worker(recv_info, &chunk) == 0);
    assert(g_mock_cuda_memcpy_calls == 1);
    assert(g_mock_cuda_last_dst == (void *)(uintptr_t)chunk.dst);
    assert(g_mock_cuda_last_src == (void *)(uintptr_t)chunk.src);
    assert(g_mock_cuda_last_count == chunk.len);
    assert(g_mock_cuda_last_kind == cudaMemcpyHostToDevice);
    assert(g_mock_cuda_last_stream.i == 9);

    assert(init_task_sync(&sync_send) == 0);
    sync_send->total_tasks = 1;
    write_info.user_ctx_server = 1;
    write_info.user_ctx_client = 2;
    construct_send_task_arg(&send_arg, write_info, &chunk, 1, true, sync_send);
    g_mock_urma_write_status = -9;
    assert(send_task_worker_func(&send_arg) == -9);
    assert(sync_send->completed_tasks == 0);
    assert(sync_send->request_completed == 1);

    assert(init_task_sync(&sync_recv) == 0);
    sync_recv->total_tasks = 1;
    construct_recv_task_arg(&recv_arg, recv_info, &chunk, true, sync_recv);
    g_mock_cuda_memcpy_ret = -3;
    assert(recv_task_worker_func(&recv_arg) == -3);
    assert(sync_recv->completed_tasks == 0);
    assert(sync_recv->request_completed == 1);

    free_sync_owned_resources(sync_send);
    free_sync_owned_resources(sync_recv);
}

/*
 * Task registration tests.
 * Verify send/recv registration error paths and task content on success.
 */
static void test_register_task_functions(void)
{
    os_transport_handle_t ost_handle = {0};
    urma_info_t urma_info = {0};
    struct chunk_info chunks[3] = {
        {.src = 0x100, .dst = 0x200, .len = 10},
        {.src = 0x110, .dst = 0x210, .len = 11},
        {.src = 0x120, .dst = 0x220, .len = 12},
    };
    task_sync_t *sync = NULL;
    send_task_arg_t *send_args = NULL;
    recv_task_arg_t *recv_args = NULL;

    reset_mocks();
    ost_handle.thread_pool = (ThreadPoolHandle)0x1234;

    assert(init_task_sync(&sync) == 0);
    urma_info.write_info.user_ctx_server = 0x55AA;
    assert(register_send_tasks(&ost_handle, chunks, 1, dummy_task_func, urma_info, sync) == -1);
    free_sync_owned_resources(sync);

    assert(init_task_sync(&sync) == 0);
    g_mock_submit_fail = 1;
    assert(register_send_tasks(&ost_handle, chunks, 2, dummy_task_func, urma_info, sync) == -1);
    assert(sync->task_group == NULL);
    g_mock_submit_fail = 0;
    free_sync_owned_resources(sync);

    assert(init_task_sync(&sync) == 0);
    assert(register_send_tasks(&ost_handle, chunks, 3, dummy_task_func, urma_info, sync) == 0);
    assert(sync->total_tasks == 2);
    assert(sync->task_group != NULL);
    assert(sync->task_group->task_num == 2);
    send_args = (send_task_arg_t *)sync->task_group->task_args;
    assert(send_args[0].chunk_info == &chunks[1]);
    assert(send_args[0].is_last_chunk == false);
    assert(send_args[1].chunk_info == &chunks[2]);
    assert(send_args[1].is_last_chunk == true);
    assert(sync->task_group->tasks[0].request_id == 0x55AA);
    assert(g_mock_last_submit_task_count == 2);
    free_sync_owned_resources(sync);

    assert(init_task_sync(&sync) == 0);
    urma_info.recv_info.request_id = 0x777;
    g_mock_submit_fail = 1;
    assert(register_recv_tasks(&ost_handle, chunks, 2, dummy_task_func, urma_info, sync) == -1);
    g_mock_submit_fail = 0;
    free_sync_owned_resources(sync);

    assert(init_task_sync(&sync) == 0);
    assert(register_recv_tasks(&ost_handle, chunks, 2, dummy_task_func, urma_info, sync) == 0);
    assert(sync->total_tasks == 2);
    assert(sync->task_group->task_num == 2);
    recv_args = (recv_task_arg_t *)sync->task_group->task_args;
    assert(recv_args[0].chunk_info == &chunks[0]);
    assert(recv_args[0].is_last_chunk == false);
    assert(recv_args[1].chunk_info == &chunks[1]);
    assert(recv_args[1].is_last_chunk == true);
    assert(sync->task_group->tasks[0].request_id == 0x777);
    free_sync_owned_resources(sync);
}

/*
 * Registration orchestration tests.
 * Verify construct_and_register_worker_task and bind-chunk wrapper behavior.
 */
static void test_construct_and_bind_functions(void)
{
    os_transport_handle_t ost_handle = {0};
    urma_info_t urma_info = {0};
    struct chunk_info chunks_send[2] = {
        {.src = 1, .dst = 2, .len = 3},
        {.src = 4, .dst = 5, .len = 6},
    };
    struct chunk_info chunks_recv[1] = {
        {.src = 7, .dst = 8, .len = 9},
    };
    task_sync_t *sync = NULL;

    reset_mocks();
    ost_handle.thread_pool = (ThreadPoolHandle)0x1234;
    urma_info.write_info.user_ctx_server = 88;
    urma_info.recv_info.request_id = 99;

    assert(construct_and_register_worker_task(NULL, chunks_send, 2, SEND_TASK,
                                              dummy_task_func, urma_info, &sync) == (uint32_t)-1);
    assert(construct_and_register_worker_task(&ost_handle, NULL, 2, SEND_TASK,
                                              dummy_task_func, urma_info, &sync) == (uint32_t)-1);
    assert(construct_and_register_worker_task(&ost_handle, chunks_send, 2, SEND_TASK,
                                              dummy_task_func, urma_info, NULL) == (uint32_t)-1);
    assert(construct_and_register_worker_task(&ost_handle, chunks_send, 0, SEND_TASK,
                                              dummy_task_func, urma_info, &sync) == (uint32_t)-1);

    assert(construct_and_register_worker_task(&ost_handle, chunks_send, 2, NULL_TASK,
                                              dummy_task_func, urma_info, &sync) == (uint32_t)-1);

    assert(construct_and_register_worker_task(&ost_handle, chunks_send, 2, SEND_TASK,
                                              dummy_task_func, urma_info, &sync) == 0);
    assert(sync != NULL);
    free_sync_owned_resources(sync);

    assert(construct_and_register_worker_task(&ost_handle, chunks_recv, 1, RECV_TASK,
                                              dummy_task_func, urma_info, &sync) == 0);
    assert(sync != NULL);
    free_sync_owned_resources(sync);

    assert(register_tasks_and_bind_chunks(&ost_handle, chunks_recv, 1, RECV_TASK,
                                          dummy_task_func, urma_info, NULL) == -1);
    assert(register_tasks_and_bind_chunks(&ost_handle, chunks_recv, 1, RECV_TASK,
                                          dummy_task_func, urma_info, &sync) == 0);
    assert(sync->chunks == chunks_recv);
    sync->chunks = NULL;
    free_sync_owned_resources(sync);
}

/*
 * Single-chunk send and JFC registration tests.
 * Verify single write behavior plus os_transport_reg_jfc gate conditions.
 */
static void test_send_single_chunk_and_reg_jfc(void)
{
    urma_jetty_info_t jetty_info = {0};
    struct buffer_info local_src = {0};
    struct buffer_info remote_dst = {0};
    struct _ThreadPool pool = {0};
    os_transport_handle_t handle = {0};

    reset_mocks();
    jetty_info.jfs = (urma_jfs_t *)0x10;
    jetty_info.jetty = (urma_jetty_t *)0x20;
    jetty_info.tjetty = (urma_target_jetty_t *)0x30;
    local_src.addr = 0x1111;
    local_src.tseg = (urma_target_seg_t *)0x40;
    remote_dst.addr = 0x2222;
    remote_dst.tseg = (urma_target_seg_t *)0x50;

    g_mock_urma_write_status = URMA_SUCCESS;
    assert(send_single_chunk(&jetty_info, &local_src, &remote_dst, 64, 7, 8) == 0);
    assert(g_mock_urma_write_calls == 1);
    assert(g_mock_last_write_chunk.src == 0x1111);
    assert(g_mock_last_write_chunk.dst == 0x2222);
    assert(g_mock_last_write_chunk.len == 64);

    g_mock_urma_write_status = -1;
    assert(send_single_chunk(&jetty_info, &local_src, &remote_dst, 64, 7, 8) == -1);

    g_inited = 0;
    assert(os_transport_reg_jfc((urma_jfce_t *)0x1, (urma_jfc_t *)0x2, &handle) == (uint32_t)-1);
    g_inited = 1;
    assert(os_transport_reg_jfc((urma_jfce_t *)0x1, (urma_jfc_t *)0x2, NULL) == (uint32_t)-1);

    handle.thread_pool = &pool;
    handle.urma_event_mode = true;
    assert(os_transport_reg_jfc((urma_jfce_t *)0xABC, (urma_jfc_t *)0xDEF, &handle) == 0);
    assert(pool.urmaInfo.jfce == (urma_jfce_t *)0xABC);
    assert(pool.urmaInfo.jfc == (urma_jfc_t *)0xDEF);
    assert(pool.urmaInfo.urma_event_mode == true);

    g_inited = 0;
}

/*
 * Public API tests for init/destroy/send/recv.
 * Covers success and representative failure paths, including large-send sync-handle semantics.
 */
static void test_init_destroy_and_send_recv_api(void)
{
    os_transport_cfg_t cfg = {0};
    uint32_t ret = 0;
    void *handle = NULL;
    void *handle2 = NULL;
    os_transport_handle_t fake = {0};
    os_transport_handle_t send_handle = {0};
    urma_jetty_info_t jetty_info = {0};
    struct buffer_info local_src = {0};
    struct buffer_info remote_dst = {0};
    struct buffer_info host_src = {0};
    ost_device_info_t device_dst = {0};
    task_sync_t *sync = (task_sync_t *)0xDEADBEEF;

    reset_mocks();
    g_inited = 0;

    /* Only ost_cfg and handle are required by current implementation. */
    assert(os_transport_init(NULL, NULL, &handle) == (uint32_t)-1);
    assert(os_transport_init(NULL, &cfg, NULL) == (uint32_t)-1);

    g_mock_pool_init_fail = 1;
    ret = os_transport_init(NULL, &cfg, &handle);
    assert(ret != 0);
    g_mock_pool_init_fail = 0;

    g_mock_pool_start_fail = 1;
    ret = os_transport_init(NULL, &cfg, &handle);
    assert(ret != 0);
    g_mock_pool_start_fail = 0;

    cfg.worker_thread_num = 4;
    cfg.urma_event_mode = true;
    cfg.jfce = (urma_jfce_t *)0x111;
    cfg.jfc = (urma_jfc_t *)0x222;
    assert(os_transport_init(NULL, &cfg, &handle) == 0);
    assert(handle != NULL);
    assert(g_inited == 1);
    assert(((os_transport_handle_t *)handle)->worker_thread_num == 4);
    assert(((os_transport_handle_t *)handle)->urma_event_mode == true);

    assert(os_transport_init(NULL, &cfg, &handle2) == (uint32_t)-1);

    assert(os_transport_destroy(NULL) == (uint32_t)-1);
    assert(os_transport_destroy(handle) == 0);
    assert(g_inited == 0);
    assert(g_mock_destroy_calls >= 1);

    fake.thread_pool = NULL;
    assert(os_transport_destroy(&fake) == (uint32_t)-1);

    reset_mocks();
    build_valid_send_args(&send_handle, &jetty_info, &local_src, &remote_dst);
    g_inited = 0;
    sync = (task_sync_t *)0xBAD;
    assert(os_transport_send(&send_handle, &jetty_info, &local_src, &remote_dst,
                             DEFAULT_CHUNK_SIZE + 1, 1, 2, &sync) == (uint32_t)-1);

    g_inited = 1;
    g_mock_urma_write_status = URMA_SUCCESS;
    assert(os_transport_send(&send_handle, &jetty_info, &local_src, &remote_dst,
                             DEFAULT_CHUNK_SIZE, 1, 2, &sync) == 0);

    g_mock_urma_write_status = -1;
    assert(os_transport_send(&send_handle, &jetty_info, &local_src, &remote_dst,
                             DEFAULT_CHUNK_SIZE, 1, 2, &sync) == (uint32_t)-1);

    reset_mocks();
    g_inited = 1;
    g_mock_submit_fail = 1;
    sync = NULL;
    assert(os_transport_send(&send_handle, &jetty_info, &local_src, &remote_dst,
                             DEFAULT_CHUNK_SIZE + 64, 100, 200, &sync) == (uint32_t)-1);
    assert(sync == NULL);

    reset_mocks();
    g_inited = 1;
    g_mock_urma_write_status = -1;
    sync = (task_sync_t *)0xAAAA;
    assert(os_transport_send(&send_handle, &jetty_info, &local_src, &remote_dst,
                             DEFAULT_CHUNK_SIZE + 64, 100, 200, &sync) == (uint32_t)-1);
    assert(sync != NULL);
    assert(wait_and_free_sync(&send_handle, sync) == (uint32_t)-1);
    sync = NULL;

    reset_mocks();
    g_inited = 1;
    sync = NULL;
    assert(os_transport_send(&send_handle, &jetty_info, &local_src, &remote_dst,
                             DEFAULT_CHUNK_SIZE + 64, 101, 201, &sync) == 0);
    assert(sync != NULL);
    /* API behavior test only: avoid asserting chunk ownership contract here. */
    sync->chunks = NULL;
    free_sync_owned_resources(sync);

    host_src.addr = 0x3300;
    device_dst.dst = (void *)0x4400;
    sync = NULL;

    g_inited = 0;
    assert(os_transport_recv(&send_handle, &host_src, &device_dst, 64, 88, &sync) == (uint32_t)-1);

    g_inited = 1;
    reset_mocks();
    g_mock_submit_fail = 1;
    assert(os_transport_recv(&send_handle, &host_src, &device_dst, 64, 88, &sync) == (uint32_t)-1);

    reset_mocks();
    assert(os_transport_recv(&send_handle, &host_src, &device_dst, 64, 88, &sync) == 0);
    assert(sync != NULL);
    /* API behavior test only: avoid asserting chunk ownership contract here. */
    sync->chunks = NULL;
    free_sync_owned_resources(sync);

    g_inited = 0;
}

/*
 * wait_and_free_sync API tests.
 * Verify null guard, success return, and failed-completion return path.
 */
static void test_wait_and_free_sync(void)
{
    task_sync_t *sync = NULL;
    os_transport_handle_t handle = {0};

    handle.thread_pool = (ThreadPoolHandle)0x1234;

    assert(wait_and_free_sync(NULL, NULL) == (uint32_t)-1);
    assert(wait_and_free_sync(&handle, NULL) == (uint32_t)-1);

    sync = create_sync_with_tasks(1, 1);
    sync->task_group->tasks[0].is_completed = true;
    sync->task_group->tasks[0].request_id = 101;
    reset_mocks();
    assert(wait_and_free_sync(&handle, sync) == 0);
    assert(g_mock_cancel_calls == 0);

    sync = create_sync_with_tasks(1, 1);
    sync->task_group->tasks[0].is_completed = false;
    sync->task_group->tasks[0].request_id = 202;
    reset_mocks();
    assert(wait_and_free_sync(&handle, sync) == (uint32_t)-1);
    assert(g_mock_cancel_calls == 1);
    assert(g_mock_cancel_last_request_id == 202);
}

#define RUN_TEST(fn) \
    do {             \
        fn();        \
    } while (0)

int main(void)
{
    /* Internal helpers and data-shaping utilities. */
    RUN_TEST(test_alloc_task_group);
    RUN_TEST(test_init_task_sync_and_free_helpers);
    RUN_TEST(test_wait_for_task_complete_and_mark);
    RUN_TEST(test_update_and_validate_and_build);
    RUN_TEST(test_split_chunk_functions);
    RUN_TEST(test_construct_and_worker_helper_functions);
    RUN_TEST(test_do_chunk_and_worker_funcs);
    RUN_TEST(test_register_task_functions);
    RUN_TEST(test_construct_and_bind_functions);

    /* Public API behavior and regression scenarios. */
    RUN_TEST(test_send_single_chunk_and_reg_jfc);
    RUN_TEST(test_init_destroy_and_send_recv_api);
    RUN_TEST(test_wait_and_free_sync);

    printf("test_os_transport_unit passed\n");
    return 0;
}

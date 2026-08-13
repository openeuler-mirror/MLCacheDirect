#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "os_transport_log_internal.h"

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
static uint64_t g_mock_last_submit_request_id = 0;
static void *g_mock_last_submit_task_arg0 = NULL;
static int g_mock_cancel_ret = 0;
static uint32_t g_mock_cancel_calls = 0;
static uint64_t g_mock_cancel_last_request_id = 0;
static uint32_t g_mock_wake_calls = 0;
static uint64_t g_mock_wake_last_request_id = 0;
static os_transport_user_data_t g_mock_wake_last_user_data;

/* URMA mock controls and observations. */
#define TEST_URMA_STATUS_FIRST  ((urma_status_t)7)
#define TEST_URMA_STATUS_SECOND ((urma_status_t)9)

static urma_status_t g_mock_urma_write_status = URMA_SUCCESS;
static urma_status_t g_mock_urma_recv_status = URMA_SUCCESS;
static uint32_t g_mock_urma_write_calls = 0;
static uint32_t g_mock_urma_recv_calls = 0;
static urma_write_info_t g_mock_last_write_info;
static urma_recv_info_t g_mock_last_recv_info;
static struct chunk_info g_mock_last_write_chunk;
static struct chunk_info g_mock_last_recv_chunk;

/* Notify callback mock controls and observations. */
static int g_mock_notify_ret = 0;
static uint32_t g_mock_notify_calls = 0;
static os_transport_user_data_t g_mock_last_notify_user_data;

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
    g_mock_wake_calls = 0;
    g_mock_wake_last_request_id = 0;
    memset(&g_mock_wake_last_user_data, 0, sizeof(g_mock_wake_last_user_data));

    g_mock_urma_write_status = URMA_SUCCESS;
    g_mock_urma_recv_status = URMA_SUCCESS;
    g_mock_urma_write_calls = 0;
    g_mock_urma_recv_calls = 0;
    memset(&g_mock_last_write_info, 0, sizeof(g_mock_last_write_info));
    memset(&g_mock_last_recv_info, 0, sizeof(g_mock_last_recv_info));
    memset(&g_mock_last_write_chunk, 0, sizeof(g_mock_last_write_chunk));
    memset(&g_mock_last_recv_chunk, 0, sizeof(g_mock_last_recv_chunk));

    g_mock_notify_ret = 0;
    g_mock_notify_calls = 0;
    memset(&g_mock_last_notify_user_data, 0, sizeof(g_mock_last_notify_user_data));
}

ThreadPoolHandle thread_pool_init(uint32_t worker_thread_num, uint32_t pending_queue_cap)
{
    (void)worker_thread_num;
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

uint64_t thread_pool_submit_task(ThreadPoolHandle handle,
                                 uint64_t request_id,
                                 int (*task_func)(void *arg),
                                 void *task_arg,
                                 TaskCompleteCb complete_cb,
                                 void *user_data)
{
    (void)handle;
    (void)request_id;
    (void)task_func;
    (void)task_arg;
    (void)complete_cb;
    (void)user_data;
    return 1;
}

uint64_t *thread_pool_submit_batch_tasks(ThreadPoolHandle handle,
                                         ThreadPoolTask *tasks,
                                         uint32_t task_count,
                                         TaskCompleteCb complete_cb,
                                         void *user_data,
                                         TaskCompleteCb batch_complete_cb,
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

int thread_pool_cancel_tasks_by_req(ThreadPoolHandle handle, uint64_t request_id)
{
    if (!handle) {
        return -1;
    }
    g_mock_cancel_calls++;
    g_mock_cancel_last_request_id = request_id;
    return g_mock_cancel_ret;
}

int thread_pool_wake_up_worker_by_req_id(ThreadPoolHandle handle, uint64_t request_id, void *user_data)
{
    if (!handle) {
        return -1;
    }
    g_mock_wake_calls++;
    g_mock_wake_last_request_id = request_id;
    if (user_data) {
        g_mock_wake_last_user_data = *(os_transport_user_data_t *)user_data;
    }
    return 0;
}

void thread_pool_destroy(ThreadPoolHandle handle)
{
    g_mock_destroy_calls++;
    free(handle);
}

urma_status_t urma_write_chunk(urma_write_info_t write_info, struct chunk_info *chunk_info)
{
    g_mock_urma_write_calls++;
    g_mock_last_write_info = write_info;
    if (chunk_info) {
        g_mock_last_write_chunk = *chunk_info;
    }
    return g_mock_urma_write_status;
}

urma_status_t urma_write_notify(urma_write_info_t write_info, struct chunk_info *chunk_info)
{
    g_mock_urma_write_calls++;
    g_mock_last_write_info = write_info;
    if (chunk_info) {
        g_mock_last_write_chunk = *chunk_info;
    }
    return g_mock_urma_write_status;
}

urma_status_t urma_recv_with_notify(urma_recv_info_t recv_info, struct chunk_info *chunk_info)
{
    g_mock_urma_recv_calls++;
    g_mock_last_recv_info = recv_info;
    if (chunk_info) {
        g_mock_last_recv_chunk = *chunk_info;
    }
    return g_mock_urma_recv_status;
}

void ost_log_write(LogLevel level, int vlevel, const char *file, int line, const char *fmt, ...)
{
    (void)level;
    (void)vlevel;
    (void)file;
    (void)line;
    (void)fmt;
}

static int test_notify_cb(void *user_data)
{
    g_mock_notify_calls++;
    if (user_data) {
        g_mock_last_notify_user_data = *(os_transport_user_data_t *)user_data;
    }
    return g_mock_notify_ret;
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
static void build_valid_send_args(os_transport_handle_t *ost_handle,
                                  urma_jetty_info_t *jetty_info,
                                  ost_buffer_info_t *local_src,
                                  ost_buffer_info_t *remote_dst)
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

    update_urma_status(sync, TEST_URMA_STATUS_FIRST);
    update_urma_status(sync, TEST_URMA_STATUS_SECOND);
    assert(sync->urma_status == TEST_URMA_STATUS_FIRST);
    update_urma_status(sync, URMA_SUCCESS);
    assert(sync->urma_status == TEST_URMA_STATUS_FIRST);
    update_urma_status(NULL, URMA_FAIL);

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
    ost_buffer_info_t local_src = {0};
    ost_buffer_info_t remote_dst = {0};
    ost_device_info_t device_dst = {0};
    urma_write_info_t write_info;
    task_sync_t *sync_handle = NULL;

    assert(update_jfc_for_poll((urma_jfce_t *)0x1, (urma_jfc_t *)0x2, true, &pool) == 0);
    assert(pool.urmaInfo.jfce == (urma_jfce_t *)0x1);
    assert(pool.urmaInfo.jfc == (urma_jfc_t *)0x2);
    assert(pool.urmaInfo.urma_event_mode == true);

    handle.thread_pool = &pool;

    g_inited = 0;
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 1, &sync_handle) == -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, &sync_handle, test_notify_cb) == -1);

    g_inited = 1;
    assert(validate_send_input(NULL, &jetty_info, &local_src, &remote_dst, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, NULL, &local_src, &remote_dst, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, NULL, &remote_dst, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, &local_src, NULL, 1, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 0, &sync_handle) == -1);
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 1, NULL) == -1);

    assert(validate_recv_input(NULL, &local_src, &device_dst, 1, &sync_handle, test_notify_cb) == -1);
    assert(validate_recv_input(&handle, NULL, &device_dst, 1, &sync_handle, test_notify_cb) == -1);
    assert(validate_recv_input(&handle, &local_src, NULL, 1, &sync_handle, test_notify_cb) == -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 0, &sync_handle, test_notify_cb) == -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, NULL, test_notify_cb) == -1);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, &sync_handle, NULL) == -1);

    device_dst.jetty = (urma_jetty_t *)0x100;
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, &sync_handle, test_notify_cb) == 0);

    jetty_info.jfs = (urma_jfs_t *)0x10;
    jetty_info.jetty = (urma_jetty_t *)0x20;
    jetty_info.tjetty = (urma_target_jetty_t *)0x30;
    local_src.tseg = (urma_target_seg_t *)0x40;
    remote_dst.tseg = (urma_target_seg_t *)0x50;

    const uint64_t raw_client_key = 0xABCDE12345ULL;
    const uint64_t encoded_client_key = 0xABCDE0000012345ULL;
    write_info = build_write_info(&jetty_info, &local_src, &remote_dst, 123, 456, raw_client_key);
    assert(write_info.jfs == jetty_info.jfs);
    assert(write_info.jetty == jetty_info.jetty);
    assert(write_info.target_jfr == jetty_info.tjetty);
    assert(write_info.src_tseg == local_src.tseg);
    assert(write_info.dst_tseg == remote_dst.tseg);
    assert(write_info.flag.value == 0);
    assert(write_info.user_ctx_server.bs.request_id == 456);
    assert(write_info.user_ctx_server.bs.chunk_type == MIDDLE_CHUNK);
    assert(write_info.user_ctx_client.bs.request_id == encoded_client_key);
    assert(write_info.user_ctx_client.bs.chunk_type == MIDDLE_CHUNK);
    assert(validate_send_input(&handle, &jetty_info, &local_src, &remote_dst, 1, &sync_handle) == 0);
    assert(validate_recv_input(&handle, &local_src, &device_dst, 1, &sync_handle, test_notify_cb) == 0);

    g_inited = 0;
}

/*
 * Request ID codec tests.
 * Verify all effective bits round-trip and unsupported high bits are rejected.
 */
static void test_request_id_codec(void)
{
    const uint64_t request_ids[] = {
        0,
        URMA_REQ_ID_FIELD_MASK,
        1ULL << URMA_REQ_ID_FIELD_WIDTH,
        0xABCDE12345ULL,
        OS_TRANSPORT_MAX_REQUEST_ID,
    };

    for (size_t i = 0; i < sizeof(request_ids) / sizeof(request_ids[0]); i++) {
        uint64_t encoded_request_id = encode_request_id(request_ids[i]);
        assert((encoded_request_id & (URMA_REQ_ID_FIELD_MASK << URMA_REQ_ID_FIELD_WIDTH)) == 0);
        assert(decode_request_id(encoded_request_id) == request_ids[i]);
        assert(is_request_id_valid(request_ids[i]));
    }

    assert(encode_request_id(0xABCDE12345ULL) == 0xABCDE0000012345ULL);
    assert(!is_request_id_valid(OS_TRANSPORT_MAX_REQUEST_ID + 1));
}

/*
 * os_transport_wake_up_task tests.
 * Verify recv/send completion decode paths and invalid-argument guards.
 */
static void test_wake_up_task(void)
{
    os_transport_handle_t handle = {0};
    os_transport_user_data_t user_data = {0};
    urma_cr_t cr = {0};

    reset_mocks();
    handle.thread_pool = (ThreadPoolHandle)0x1234;

    assert(os_transport_wake_up_task(NULL, &cr) == -1);
    assert(os_transport_wake_up_task(&handle, NULL) == -1);

    /* recv 路径：未知 opcode 返回错误 */
    cr.flag.bs.s_r = 1;
    cr.opcode = URMA_CR_OPC_WRITE_WITH_IMM;
    assert(os_transport_wake_up_task(&handle, &cr) == -1);

    /* send 路径：从 user_ctx 还原 request_id 并唤醒对应 worker */
    const uint64_t raw_request_id = 0xABCDE12345ULL;
    user_data.bs.request_id = raw_request_id;
    user_data.bs.chunk_id = 5;
    user_data.bs.chunk_type = LAST_CHUNK;
    memset(&cr, 0, sizeof(cr));
    cr.opcode = URMA_CR_OPC_SEND;
    cr.user_ctx = user_data.user_ctx;
    assert(os_transport_wake_up_task(&handle, &cr) == 0);
    assert(g_mock_wake_calls == 1);
    assert(g_mock_wake_last_request_id == raw_request_id);
    assert(g_mock_wake_last_user_data.user_ctx == user_data.user_ctx);

    /* send 路径：chunk_id 超界（尾片完成通知）被忽略，不唤醒 worker */
    user_data.bs.chunk_id = OS_TRANSPORT_MAX_CHUNK_NUM;
    memset(&cr, 0, sizeof(cr));
    cr.opcode = URMA_CR_OPC_WRITE_WITH_IMM;
    cr.user_ctx = user_data.user_ctx;
    assert(os_transport_wake_up_task(&handle, &cr) == 0);
    assert(g_mock_wake_calls == 1);

    /* recv 路径：从 imm_data 解码 request_id 并唤醒对应 worker */
    user_data.bs.request_id = encode_request_id(raw_request_id);
    user_data.bs.chunk_id = 7;
    user_data.bs.chunk_type = LAST_CHUNK;
    memset(&cr, 0, sizeof(cr));
    cr.flag.bs.s_r = 1;
    cr.opcode = URMA_CR_OPC_SEND_WITH_IMM;
    cr.imm_data = user_data.user_ctx;
    assert(os_transport_wake_up_task(&handle, &cr) == 0);
    assert(g_mock_wake_calls == 2);
    assert(g_mock_wake_last_request_id == raw_request_id);
    assert(g_mock_wake_last_user_data.bs.request_id == raw_request_id);
    assert(g_mock_wake_last_user_data.bs.chunk_id == 7);
    assert(g_mock_wake_last_user_data.bs.chunk_type == LAST_CHUNK);
    /* recv 路径会补充投递一个 recv（recv_ctx_add） */
    assert(g_mock_urma_recv_calls == 1);

    /* 无线程池时唤醒失败 */
    handle.thread_pool = NULL;
    user_data.bs.request_id = raw_request_id;
    user_data.bs.chunk_id = 5;
    memset(&cr, 0, sizeof(cr));
    cr.opcode = URMA_CR_OPC_SEND;
    cr.user_ctx = user_data.user_ctx;
    assert(os_transport_wake_up_task(&handle, &cr) == -1);
    assert(g_mock_wake_calls == 2);
}

/*
 * Chunk split tests.
 * Verify split counts, address arithmetic, and invalid-argument guards.
 */
static void test_split_chunk_functions(void)
{
    struct chunk_info *chunks = NULL;
    uint32_t chunk_num = 0;
    ost_buffer_info_t local = {0};
    ost_buffer_info_t remote = {0};
    ost_buffer_info_t host = {0};
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

    write_info.user_ctx_server.bs.request_id = 77;
    write_info.user_ctx_client.bs.request_id = 99;
    construct_send_task_arg(&send_arg, write_info, &chunk, 5, false, sync);
    user_server = send_arg.write_info.user_ctx_server;
    user_client = send_arg.write_info.user_ctx_client;
    assert(user_server.bs.chunk_type == MIDDLE_CHUNK);
    assert(user_server.bs.chunk_id == 5);
    assert(user_server.bs.request_id == 77);
    assert(user_client.bs.chunk_type == MIDDLE_CHUNK);
    assert(user_client.bs.request_id == 99);

    construct_send_task_arg(&send_arg, write_info, &chunk, 6, true, sync);
    user_server = send_arg.write_info.user_ctx_server;
    assert(user_server.bs.chunk_type == LAST_CHUNK);
    assert(user_server.bs.chunk_id == 6);

    construct_send_task_arg(&send_arg, write_info, &chunk, OS_TRANSPORT_MAX_CHUNK_ID, true, sync);
    user_server = send_arg.write_info.user_ctx_server;
    assert(user_server.bs.chunk_id == OS_TRANSPORT_MAX_CHUNK_ID);

    memset(&recv_arg, 0xAB, sizeof(recv_arg));
    recv_info.request_id = 55;
    recv_info.device_info.dst = (void *)0x12345;
    construct_recv_task_arg(&recv_arg, recv_info, &chunk, true, sync, test_notify_cb, 0x12345678ULL);
    assert(recv_arg.recv_info.request_id == 55);
    assert(recv_arg.expected_imm64 == 0x12345678ULL);
    assert(recv_arg.chunk_info == &chunk);
    assert(recv_arg.is_last_chunk == true);
    assert(recv_arg.sync == sync);
    assert(recv_arg.notify_callback == test_notify_cb);

    worker_task = construct_worker_task(100, 200, dummy_task_func, &chunk, prepare_recv_task_user_data);
    assert(worker_task.task_id == 100);
    assert(worker_task.request_id == 200);
    assert(worker_task.task_func == dummy_task_func);
    assert(worker_task.task_arg == &chunk);
    assert(worker_task.prepare_cb == prepare_recv_task_user_data);
    assert(worker_task.is_completed == false);
    assert(worker_task.free_task_self == false);

    free_sync_owned_resources(sync);
}

static void test_user_data_bitfield_limits(void)
{
    os_transport_user_data_t user_data = {0};

    assert(OS_TRANSPORT_MAX_CHUNK_ID == 6U);
    assert(OS_TRANSPORT_MAX_CHUNK_NUM == 7U);

    user_data.bs.chunk_id = OS_TRANSPORT_MAX_CHUNK_ID;
    assert(user_data.bs.chunk_id == OS_TRANSPORT_MAX_CHUNK_ID);
}

/*
 * Worker wrapper tests.
 * Verify send/recv worker wrappers forward return codes and mark completion.
 */
static void test_do_chunk_and_worker_funcs(void)
{
    task_sync_t *sync_send = NULL;
    task_sync_t *sync_recv = NULL;
    task_sync_t *sync_recv_fail = NULL;
    struct chunk_info chunk = {.src = 0x1, .dst = 0x2, .len = 8};
    urma_write_info_t write_info = {0};
    urma_recv_info_t recv_info = {0};
    send_task_arg_t send_arg = {0};
    recv_task_arg_t recv_arg = {0};
    recv_task_arg_t recv_fail_arg = {0};
    os_transport_user_data_t notify_data = {0};

    reset_mocks();
    g_mock_urma_write_status = 7;
    assert(do_send_chunk_for_worker(write_info, &chunk) == 7);
    assert(g_mock_urma_write_calls == 1);

    assert(send_task_worker_func(NULL) == -1);
    assert(recv_task_worker_func(NULL) == -1);

    assert(init_task_sync(&sync_send) == 0);
    sync_send->total_tasks = 1;
    write_info.user_ctx_server.bs.request_id = 1;
    write_info.user_ctx_client.bs.request_id = 2;
    construct_send_task_arg(&send_arg, write_info, &chunk, 1, true, sync_send);
    g_mock_urma_write_status = -9;
    assert(send_task_worker_func(&send_arg) == -9);
    assert(sync_send->completed_tasks == 0);
    assert(sync_send->request_completed == 1);
    assert(sync_send->urma_status == (urma_status_t)-9);

    assert(init_task_sync(&sync_recv) == 0);
    sync_recv->total_tasks = 1;
    recv_info.request_id = 55;
    construct_recv_task_arg(&recv_arg, recv_info, &chunk, true, sync_recv, test_notify_cb, 0);
    notify_data.bs.request_id = 55;
    notify_data.bs.chunk_id = 3;
    notify_data.bs.chunk_type = LAST_CHUNK;
    prepare_recv_task_user_data(&recv_arg, &notify_data);
    assert(recv_task_worker_func(&recv_arg) == 0);
    assert(g_mock_notify_calls == 1);
    assert(g_mock_last_notify_user_data.user_ctx == notify_data.user_ctx);
    assert(sync_recv->completed_tasks == 1);
    assert(sync_recv->request_completed == 1);
    assert(sync_recv->urma_status == URMA_SUCCESS);

    g_mock_notify_ret = 0;
    assert(init_task_sync(&sync_recv_fail) == 0);
    sync_recv_fail->total_tasks = 1;
    construct_recv_task_arg(&recv_fail_arg, recv_info, &chunk, true, sync_recv_fail, NULL, 0);
    prepare_recv_task_user_data(&recv_fail_arg, &notify_data);
    assert(recv_task_worker_func(&recv_fail_arg) == -1);
    assert(sync_recv_fail->request_canceled == 1);
    assert(sync_recv_fail->request_completed == 1);
    assert(sync_recv_fail->urma_status == URMA_SUCCESS);
    free_sync_owned_resources(sync_recv_fail);
    sync_recv_fail = NULL;

    assert(init_task_sync(&sync_recv_fail) == 0);
    sync_recv_fail->total_tasks = 1;
    construct_recv_task_arg(&recv_fail_arg, recv_info, &chunk, true, sync_recv_fail, test_notify_cb, 0);
    prepare_recv_task_user_data(&recv_fail_arg, &notify_data);
    g_mock_notify_ret = -3;
    assert(recv_task_worker_func(&recv_fail_arg) == -3);
    assert(sync_recv_fail->completed_tasks == 0);
    assert(sync_recv_fail->request_completed == 1);
    assert(sync_recv_fail->urma_status == URMA_SUCCESS);

    free_sync_owned_resources(sync_send);
    free_sync_owned_resources(sync_recv);
    free_sync_owned_resources(sync_recv_fail);
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
    urma_info.write_info.user_ctx_server.bs.request_id = 0x3FF;
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
    assert(sync->total_tasks == 3);
    assert(sync->task_group != NULL);
    assert(sync->task_group->task_num == 3);
    send_args = (send_task_arg_t *)sync->task_group->task_args;
    assert(send_args[0].chunk_info == &chunks[1]);
    assert(send_args[0].is_last_chunk == false);
    assert(send_args[1].chunk_info == &chunks[2]);
    assert(send_args[1].is_last_chunk == true);
    /* 最后一个task承接尾片本地完成通知，不带chunk */
    assert(send_args[2].chunk_info == NULL);
    assert(send_args[2].is_last_chunk == true);
    assert(sync->task_group->tasks[0].request_id == 0x3FF);
    assert(g_mock_last_submit_task_count == 3);
    free_sync_owned_resources(sync);

    assert(init_task_sync(&sync) == 0);
    urma_info.recv_info.request_id = 0x777;
    g_mock_submit_fail = 1;
    assert(register_recv_tasks(&ost_handle, chunks, 2, dummy_task_func, urma_info, sync, test_notify_cb) == -1);
    g_mock_submit_fail = 0;
    free_sync_owned_resources(sync);

    assert(init_task_sync(&sync) == 0);
    assert(register_recv_tasks(&ost_handle, chunks, 2, dummy_task_func, urma_info, sync, test_notify_cb) == 0);
    assert(sync->total_tasks == 2);
    assert(sync->task_group->task_num == 2);
    recv_args = (recv_task_arg_t *)sync->task_group->task_args;
    assert(recv_args[0].chunk_info == &chunks[0]);
    assert(recv_args[0].is_last_chunk == false);
    assert(recv_args[0].notify_callback == test_notify_cb);
    assert(recv_args[1].chunk_info == &chunks[1]);
    assert(recv_args[1].is_last_chunk == true);
    assert(recv_args[1].notify_callback == test_notify_cb);
    assert(sync->task_group->tasks[0].request_id == 0x777);
    assert(sync->task_group->tasks[0].prepare_cb == prepare_recv_task_user_data);
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
    urma_info.write_info.user_ctx_server.bs.request_id = 88;
    urma_info.recv_info.request_id = 99;

    assert(construct_and_register_worker_task(NULL, chunks_send, 2, SEND_TASK, dummy_task_func, urma_info, &sync, NULL)
           == (uint32_t)-1);
    assert(construct_and_register_worker_task(&ost_handle, NULL, 2, SEND_TASK, dummy_task_func, urma_info, &sync, NULL)
           == (uint32_t)-1);
    assert(construct_and_register_worker_task(
               &ost_handle, chunks_send, 2, SEND_TASK, dummy_task_func, urma_info, NULL, NULL)
           == (uint32_t)-1);
    assert(construct_and_register_worker_task(
               &ost_handle, chunks_send, 0, SEND_TASK, dummy_task_func, urma_info, &sync, NULL)
           == (uint32_t)-1);

    assert(construct_and_register_worker_task(
               &ost_handle, chunks_send, 2, NULL_TASK, dummy_task_func, urma_info, &sync, NULL)
           == (uint32_t)-1);

    assert(construct_and_register_worker_task(
               &ost_handle, chunks_send, 2, SEND_TASK, dummy_task_func, urma_info, &sync, NULL)
           == 0);
    assert(sync != NULL);
    free_sync_owned_resources(sync);

    assert(construct_and_register_worker_task(
               &ost_handle, chunks_recv, 1, RECV_TASK, dummy_task_func, urma_info, &sync, test_notify_cb)
           == 0);
    assert(sync != NULL);
    free_sync_owned_resources(sync);

    assert(register_tasks_and_bind_chunks(
               &ost_handle, chunks_recv, 1, RECV_TASK, dummy_task_func, urma_info, NULL, test_notify_cb)
           == -1);
    assert(register_tasks_and_bind_chunks(
               &ost_handle, chunks_recv, 1, RECV_TASK, dummy_task_func, urma_info, &sync, test_notify_cb)
           == 0);
    assert(sync->chunks == chunks_recv);
    sync->chunks = NULL;
    free_sync_owned_resources(sync);
}

/*
 * os_transport_reg_jfc gate and registration tests.
 * Verify jfc binding is gated on initialization and handle validity.
 */
static void test_reg_jfc(void)
{
    struct _ThreadPool pool = {0};
    os_transport_handle_t handle = {0};

    reset_mocks();
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
    ost_buffer_info_t local_src = {0};
    ost_buffer_info_t remote_dst = {0};
    ost_buffer_info_t host_src = {0};
    ost_device_info_t device_dst = {0};
    task_sync_t *sync = (task_sync_t *)0xDEADBEEF;
    urma_status_t urma_status = TEST_URMA_STATUS_FIRST;

    reset_mocks();
    g_inited = 0;

    /* Only ost_cfg and handle are required by current implementation. */
    assert(os_transport_init(NULL, NULL, &handle) == (uint32_t)-1);
    assert(os_transport_init(NULL, &cfg, NULL) == (uint32_t)-1);

    cfg.worker_thread_num = 4;
    cfg.urma_event_mode = true;
    cfg.jfce = (urma_jfce_t *)0x111;
    cfg.jfc = (urma_jfc_t *)0x222;

    g_mock_pool_init_fail = 1;
    ret = os_transport_init(NULL, &cfg, &handle);
    assert(ret != 0);
    g_mock_pool_init_fail = 0;

    g_mock_pool_start_fail = 1;
    ret = os_transport_init(NULL, &cfg, &handle);
    assert(ret != 0);
    g_mock_pool_start_fail = 0;

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
    assert(os_transport_send(
               &send_handle, &jetty_info, &local_src, &remote_dst, DEFAULT_CHUNK_SIZE + 1, 1, 2, &sync, &urma_status)
           == (uint32_t)-1);
    assert(urma_status == URMA_SUCCESS);

    g_inited = 1;
    sync = (task_sync_t *)0xBAD;
    assert(os_transport_send(&send_handle,
                             &jetty_info,
                             &local_src,
                             &remote_dst,
                             DEFAULT_CHUNK_SIZE,
                             OS_TRANSPORT_MAX_REQUEST_ID + 1,
                             2,
                             &sync,
                             &urma_status)
           == (uint32_t)-1);
    assert(sync == NULL);
    assert(g_mock_urma_write_calls == 0);
    assert(os_transport_send(&send_handle,
                             &jetty_info,
                             &local_src,
                             &remote_dst,
                             DEFAULT_CHUNK_SIZE,
                             1,
                             OS_TRANSPORT_MAX_REQUEST_ID + 1,
                             &sync,
                             &urma_status)
           == (uint32_t)-1);
    assert(g_mock_urma_write_calls == 0);

    g_mock_urma_write_status = URMA_SUCCESS;
    assert(os_transport_send(
               &send_handle, &jetty_info, &local_src, &remote_dst, DEFAULT_CHUNK_SIZE + 1, 1, 2, &sync, &urma_status)
           == 0);
    assert(urma_status == URMA_SUCCESS);

    g_mock_urma_write_status = TEST_URMA_STATUS_FIRST;
    assert(os_transport_send(
               &send_handle, &jetty_info, &local_src, &remote_dst, DEFAULT_CHUNK_SIZE + 1, 1, 2, &sync, &urma_status)
           == (uint32_t)-1);
    assert(urma_status == TEST_URMA_STATUS_FIRST);

    reset_mocks();
    g_inited = 1;
    g_mock_submit_fail = 1;
    sync = NULL;
    urma_status = TEST_URMA_STATUS_FIRST;
    assert(
        os_transport_send(
            &send_handle, &jetty_info, &local_src, &remote_dst, DEFAULT_CHUNK_SIZE + 64, 100, 200, &sync, &urma_status)
        == (uint32_t)-1);
    assert(sync == NULL);
    assert(urma_status == URMA_SUCCESS);

    reset_mocks();
    g_inited = 1;
    g_mock_urma_write_status = TEST_URMA_STATUS_SECOND;
    g_mock_cancel_ret = 1;
    sync = (task_sync_t *)0xAAAA;
    assert(
        os_transport_send(
            &send_handle, &jetty_info, &local_src, &remote_dst, DEFAULT_CHUNK_SIZE + 64, 100, 200, &sync, &urma_status)
        == (uint32_t)-1);
    assert(urma_status == TEST_URMA_STATUS_SECOND);
    assert(sync != NULL);
    urma_status = TEST_URMA_STATUS_FIRST;
    assert(wait_and_free_sync(&send_handle, sync, &urma_status) == (uint32_t)-1);
    assert(urma_status == URMA_SUCCESS);
    assert(g_mock_cancel_calls == 1);
    assert(g_mock_cancel_last_request_id == 100);
    sync = NULL;

    reset_mocks();
    g_inited = 1;
    sync = NULL;
    assert(
        os_transport_send(
            &send_handle, &jetty_info, &local_src, &remote_dst, DEFAULT_CHUNK_SIZE + 64, 101, 201, &sync, &urma_status)
        == 0);
    assert(urma_status == URMA_SUCCESS);
    assert(sync != NULL);
    /* API behavior test only: avoid asserting chunk ownership contract here. */
    sync->chunks = NULL;
    free_sync_owned_resources(sync);

    host_src.addr = 0x3300;
    host_src.tseg = (urma_target_seg_t *)0x3301;
    device_dst.dst = (void *)0x4400;
    device_dst.jetty = (urma_jetty_t *)0x4401;
    device_dst.jfr = (urma_jfr_t *)0x4402;
    sync = NULL;

    g_inited = 0;
    assert(os_transport_recv(&send_handle, &host_src, &device_dst, 64, 88, &sync, test_notify_cb) == (uint32_t)-1);

    g_inited = 1;
    reset_mocks();
    assert(os_transport_recv(&send_handle,
                             &host_src,
                             &device_dst,
                             64,
                             OS_TRANSPORT_MAX_REQUEST_ID + 1,
                             &sync,
                             test_notify_cb)
           == (uint32_t)-1);
    assert(sync == NULL);
    assert(g_mock_urma_recv_calls == 0);

    reset_mocks();
    g_mock_submit_fail = 1;
    assert(os_transport_recv(&send_handle, &host_src, &device_dst, 64, 88, &sync, test_notify_cb) == (uint32_t)-1);

    reset_mocks();
    /* 栈上 handle 的 recv_ctx 未初始化，需与 os_transport_init 保持一致 */
    pthread_spin_init(&send_handle.recv_ctx.lock, PTHREAD_PROCESS_PRIVATE);
    /* recv 注册成功后通过 recv_ctx 预填 recv_queue_capacity * RQE_PREFILL_MULTIPLE_DUPLEX 个 recv */
    send_handle.recv_ctx.recv_queue_capacity = 1;
    assert(os_transport_recv(&send_handle, &host_src, &device_dst, 64, 88, &sync, test_notify_cb) == 0);
    assert(sync != NULL);
    assert(g_mock_urma_recv_calls == RQE_PREFILL_MULTIPLE_DUPLEX);
    assert(g_mock_last_recv_info.request_id == 88);
    assert(g_mock_last_recv_info.jetty == device_dst.jetty);
    /* 预填的 recv 使用空 chunk */
    assert(g_mock_last_recv_chunk.src == 0);
    assert(g_mock_last_recv_chunk.dst == 0);
    assert(g_mock_last_recv_chunk.len == 0);
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
    urma_status_t urma_status = TEST_URMA_STATUS_FIRST;

    handle.thread_pool = (ThreadPoolHandle)0x1234;

    assert(wait_and_free_sync(NULL, NULL, &urma_status) == (uint32_t)-1);
    assert(urma_status == URMA_SUCCESS);
    urma_status = TEST_URMA_STATUS_FIRST;
    assert(wait_and_free_sync(&handle, NULL, &urma_status) == (uint32_t)-1);
    assert(urma_status == URMA_SUCCESS);

    assert(init_task_sync(&sync) == 0);
    assert(wait_and_free_sync(&handle, sync, &urma_status) == (uint32_t)-1);
    assert(urma_status == URMA_SUCCESS);
    free_sync_owned_resources(sync);

    sync = create_sync_with_tasks(1, 1);
    sync->completed_tasks = 1;
    sync->task_group->tasks[0].is_completed = true;
    sync->task_group->tasks[0].request_id = 101;
    reset_mocks();
    assert(wait_and_free_sync(&handle, sync, &urma_status) == 0);
    assert(urma_status == URMA_SUCCESS);
    assert(g_mock_cancel_calls == 0);

    sync = create_sync_with_tasks(1, 1);
    sync->task_group->tasks[0].is_completed = false;
    sync->task_group->tasks[0].request_id = 202;
    reset_mocks();
    assert(wait_and_free_sync(&handle, sync, &urma_status) == (uint32_t)-1);
    assert(urma_status == URMA_SUCCESS);
    assert(g_mock_cancel_calls == 1);
    assert(g_mock_cancel_last_request_id == 202);

    sync = create_sync_with_tasks(1, 1);
    sync->completed_tasks = 1;
    sync->task_group->tasks[0].request_id = 250;
    sync->urma_status = TEST_URMA_STATUS_FIRST;
    reset_mocks();
    assert(wait_and_free_sync(&handle, sync, &urma_status) == (uint32_t)-1);
    assert(urma_status == TEST_URMA_STATUS_FIRST);

    sync = create_sync_with_tasks(1, 0);
    sync->task_group->tasks[0].request_id = 303;
    reset_mocks();
    assert(wait_and_free_sync_timeout(&handle, sync, 0, &urma_status) == OS_TRANSPORT_WAIT_TIMEOUT);
    assert(urma_status == URMA_SUCCESS);
    assert(g_mock_cancel_calls == 1);
    assert(g_mock_cancel_last_request_id == 303);
    assert(sync->request_timedout == 1);
    free_sync_owned_resources(sync);
}

/*
 * Cancel API tests.
 * Verify initialization gates, thread-pool cancellation errors, and sync ownership transfer.
 */
static void test_os_transport_cancel_tasks(void)
{
    os_transport_handle_t handle = {0};
    task_sync_t *sync = NULL;

    handle.thread_pool = (ThreadPoolHandle)0x1234;
    reset_mocks();

    g_inited = 0;
    assert(os_transport_cancel_tasks(NULL, NULL, 10) == (uint32_t)-1);
    assert(os_transport_cancel_tasks(&handle, NULL, 10) == (uint32_t)-1);

    g_inited = 1;
    g_mock_cancel_ret = -1;
    assert(os_transport_cancel_tasks(&handle, NULL, 10) == (uint32_t)-1);
    assert(g_mock_cancel_calls == 1);

    g_mock_cancel_ret = 0;
    assert(os_transport_cancel_tasks(&handle, NULL, 11) == 0);

    sync = create_sync_with_tasks(1, 0);
    sync->task_group->tasks[0].request_id = 12;
    g_mock_cancel_ret = 1;
    assert(os_transport_cancel_tasks(&handle, &sync, 12) == 0);
    assert(sync == NULL);

    g_inited = 0;
}

#define RUN_TEST(fn)                                                                                                   \
    do {                                                                                                               \
        fn();                                                                                                          \
    } while (0)

int main(void)
{
    /* Internal helpers and data-shaping utilities. */
    RUN_TEST(test_alloc_task_group);
    RUN_TEST(test_init_task_sync_and_free_helpers);
    RUN_TEST(test_wait_for_task_complete_and_mark);
    RUN_TEST(test_update_and_validate_and_build);
    RUN_TEST(test_request_id_codec);
    RUN_TEST(test_wake_up_task);
    RUN_TEST(test_split_chunk_functions);
    RUN_TEST(test_construct_and_worker_helper_functions);
    RUN_TEST(test_user_data_bitfield_limits);
    RUN_TEST(test_do_chunk_and_worker_funcs);
    RUN_TEST(test_register_task_functions);
    RUN_TEST(test_construct_and_bind_functions);

    /* Public API behavior and regression scenarios. */
    RUN_TEST(test_reg_jfc);
    RUN_TEST(test_init_destroy_and_send_recv_api);
    RUN_TEST(test_wait_and_free_sync);
    RUN_TEST(test_os_transport_cancel_tasks);

    printf("test_os_transport_unit passed\n");
    return 0;
}

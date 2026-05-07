#include "os_transport_internal.h"
#include "os_transport_log_internal.h"
#include "os_transport_thread_pool_internal.h"
#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

// 全局初始化状态
static int g_inited = 0;

#define OS_TRANSPORT_MAX_CHUNK_ID ((1ULL << 6) - 1)
#define OS_TRANSPORT_WAIT_TIMEOUT 1U
#define OS_TRANSPORT_WAIT_ERROR   ((uint32_t)-1)

// init和destroy接收队列限制器，ost_handle外部调用时已校验非空
static int init_recv_queue_limiter(os_transport_handle_t *ost_handle, uint32_t recv_queue_capacity)
{
    int ret;
    uint32_t capacity = recv_queue_capacity == 0 ? DEFAULT_RECV_QUEUE_CAPACITY : recv_queue_capacity;

    ret = pthread_mutex_init(&ost_handle->recv_queue_mutex, NULL);
    if (ret != 0) {
        OST_LOG_ERROR("Failed: pthread_mutex_init returned %d in init_recv_queue_limiter.", ret);
        return -1;
    }

    ost_handle->recv_queue_available = capacity;
    ost_handle->recv_queue_acquired = 0;
    OST_LOG_INFO("Recv queue limiter enabled (available=%u, acquired=%u).",
                 ost_handle->recv_queue_available,
                 ost_handle->recv_queue_acquired);
    return 0;
}

static void destroy_recv_queue_limiter(os_transport_handle_t *ost_handle)
{
    pthread_mutex_destroy(&ost_handle->recv_queue_mutex);
}

static int acquire_recv_queue_resources(os_transport_handle_t *ost_handle,
                                        uint32_t count,
                                        uint32_t *reused_count,
                                        uint32_t *post_count)
{
    uint32_t reused;
    uint32_t required_new;

    if (!ost_handle || !reused_count || !post_count) {
        return -1;
    }

    pthread_mutex_lock(&ost_handle->recv_queue_mutex);
    reused = ost_handle->recv_queue_acquired < count ? ost_handle->recv_queue_acquired : count;
    required_new = count - reused;
    if (ost_handle->recv_queue_available < required_new) {
        OST_LOG_WARN("Recv queue resources are insufficient (available=%u, acquired=%u, requested=%u, reusable=%u).",
                     ost_handle->recv_queue_available,
                     ost_handle->recv_queue_acquired,
                     count,
                     reused);
        pthread_mutex_unlock(&ost_handle->recv_queue_mutex);
        return -1;
    }

    ost_handle->recv_queue_acquired -= reused;
    ost_handle->recv_queue_available -= required_new;
    pthread_mutex_unlock(&ost_handle->recv_queue_mutex);

    *reused_count = reused;
    *post_count = required_new;
    return 0;
}

static void release_recv_queue_resources(os_transport_handle_t *ost_handle, uint32_t available, uint32_t acquired)
{
    if (!ost_handle || (available == 0 && acquired == 0)) {
        return;
    }

    pthread_mutex_lock(&ost_handle->recv_queue_mutex);
    ost_handle->recv_queue_available += available;
    ost_handle->recv_queue_acquired += acquired;
    pthread_mutex_unlock(&ost_handle->recv_queue_mutex);
}

static int alloc_task_group(task_group_t **task_group_out, uint32_t task_num, uint32_t task_arg_size)
{
    task_group_t *task_group = NULL;

    if (!task_group_out || task_num == 0 || task_arg_size == 0) {
        OST_LOG_ERROR("Failed: invalid arguments for alloc_task_group "
                      "(task_group_out=%p, task_num=%u, task_arg_size=%zu).",
                      (void *)task_group_out,
                      task_num,
                      task_arg_size);
        return -1;
    }

    task_group = calloc(1, sizeof(task_group_t));
    if (!task_group) {
        OST_LOG_ERROR("Failed: unable to allocate task_group_t (size=%zu).", sizeof(task_group_t));
        return -1;
    }

    task_group->tasks = calloc(task_num, sizeof(ThreadPoolTask));
    task_group->task_args = calloc(task_num, task_arg_size);
    if (!task_group->tasks || !task_group->task_args) {
        OST_LOG_ERROR("Failed: unable to allocate task group buffers "
                      "(task_num=%u, task_arg_size=%zu).",
                      task_num,
                      task_arg_size);
        free(task_group->task_args);
        free(task_group->tasks);
        free(task_group);
        return -1;
    }
    task_group->task_num = task_num;

    *task_group_out = task_group;
    return 0;
}

static int init_task_sync(task_sync_t **sync_out)
{
    task_sync_t *sync = NULL;
    pthread_condattr_t cond_attr;
    int ret;
    bool cond_attr_inited = false;

    if (!sync_out) {
        OST_LOG_ERROR("Failed: sync_out is NULL in init_task_sync.");
        return -1;
    }

    sync = calloc(1, sizeof(task_sync_t));
    if (!sync) {
        OST_LOG_ERROR("Failed: unable to allocate task_sync_t (size=%zu).", sizeof(task_sync_t));
        return -1;
    }

    ret = pthread_mutex_init(&sync->mutex, NULL);
    if (ret != 0) {
        OST_LOG_ERROR("Failed: pthread_mutex_init returned %d in init_task_sync.", ret);
        free(sync);
        return -1;
    }

    ret = pthread_condattr_init(&cond_attr);
    if (ret == 0) {
        cond_attr_inited = true;
#ifdef CLOCK_MONOTONIC
        ret = pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC);
        if (ret != 0) {
            OST_LOG_ERROR("Failed: pthread_condattr_setclock(CLOCK_MONOTONIC) returned %d.", ret);
            pthread_condattr_destroy(&cond_attr);
            pthread_mutex_destroy(&sync->mutex);
            free(sync);
            return -1;
        }
#endif
    } else {
        OST_LOG_WARN("pthread_condattr_init returned %d, fallback to default cond attr.", ret);
    }

    ret = pthread_cond_init(&sync->cond, cond_attr_inited ? &cond_attr : NULL);
    if (cond_attr_inited) {
        pthread_condattr_destroy(&cond_attr);
    }
    if (ret != 0) {
        OST_LOG_ERROR("Failed: pthread_cond_init returned %d in init_task_sync.", ret);
        pthread_mutex_destroy(&sync->mutex);
        free(sync);
        return -1;
    }

    *sync_out = sync;
    return 0;
}

static void free_task_group_resource(task_sync_t *sync)
{
    task_group_t *task_group;

    if (!sync || !sync->task_group) {
        return;
    }

    task_group = sync->task_group;
    free(task_group->task_args);
    free(task_group->tasks);
    free(task_group);
    sync->task_group = NULL;
}

static void free_sync_owned_resources(task_sync_t *sync)
{
    if (!sync) {
        return;
    }

    free_task_group_resource(sync);
    free(sync->chunks);
    sync->chunks = NULL;
    pthread_mutex_destroy(&sync->mutex);
    pthread_cond_destroy(&sync->cond);
    free(sync);
}

static bool sync_all_tasks_accounted_locked(task_sync_t *sync)
{
    return sync->total_tasks == 0 || (sync->completed_tasks + sync->canceled_tasks) >= sync->total_tasks;
}

static bool sync_should_free_locked(task_sync_t *sync)
{
    return sync->release_requested && sync->request_completed && sync_all_tasks_accounted_locked(sync) && !sync->freeing;
}

static void sync_free_if_ready(task_sync_t *sync)
{
    bool should_free = false;

    if (!sync) {
        return;
    }

    pthread_mutex_lock(&sync->mutex);
    if (sync_should_free_locked(sync)) {
        sync->freeing = 1;
        should_free = true;
    }
    pthread_mutex_unlock(&sync->mutex);

    if (should_free) {
        free_sync_owned_resources(sync);
    }
}

static void sync_request_release(task_sync_t *sync)
{
    if (!sync) {
        return;
    }
    pthread_mutex_lock(&sync->mutex);
    sync->release_requested = 1;
    pthread_mutex_unlock(&sync->mutex);
    sync_free_if_ready(sync);
}

static void make_abs_timeout_ms(int64_t timeout_ms, struct timespec *ts)
{
#ifdef CLOCK_MONOTONIC
    clock_gettime(CLOCK_MONOTONIC, ts);
#else
    clock_gettime(CLOCK_REALTIME, ts);
#endif
    ts->tv_sec += timeout_ms / 1000;
    ts->tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) {
        ts->tv_sec += ts->tv_nsec / 1000000000L;
        ts->tv_nsec %= 1000000000L;
    }
}

static uint32_t wait_for_task_complete_common(task_sync_t *sync_handle, int64_t timeout_ms)
{
    int ret = 0;
    bool timed_wait = (timeout_ms >= 0);
    struct timespec deadline = {0};

    if (!sync_handle) {
        OST_LOG_ERROR("Failed: sync_handle is NULL in wait_for_task_complete_common.");
        return OS_TRANSPORT_WAIT_ERROR;
    }

    if (timed_wait) {
        make_abs_timeout_ms(timeout_ms, &deadline);
    }

    pthread_mutex_lock(&sync_handle->mutex);
    while (!sync_handle->request_completed) {
        if (timed_wait) {
            ret = pthread_cond_timedwait(&sync_handle->cond, &sync_handle->mutex, &deadline);
            if (ret == ETIMEDOUT) {
                sync_handle->request_timedout = 1;
                pthread_mutex_unlock(&sync_handle->mutex);
                return OS_TRANSPORT_WAIT_TIMEOUT;
            }
        } else {
            ret = pthread_cond_wait(&sync_handle->cond, &sync_handle->mutex);
        }
        if (ret != 0) {
            pthread_mutex_unlock(&sync_handle->mutex);
            OST_LOG_ERROR("Failed: pthread_cond_wait/timedwait returned %d while waiting for request completion.", ret);
            return OS_TRANSPORT_WAIT_ERROR;
        }
    }

    bool success = (sync_handle->completed_tasks == sync_handle->total_tasks) && !sync_handle->request_canceled
                   && !sync_handle->request_timedout;
    if (!success) {
        OST_LOG_WARN("Request completed early/incompletely/canceled "
                     "(completed=%lu, canceled=%lu, total=%lu, canceled_flag=%d, timedout=%d).",
                     sync_handle->completed_tasks,
                     sync_handle->canceled_tasks,
                     sync_handle->total_tasks,
                     sync_handle->request_canceled,
                     sync_handle->request_timedout);
    }
    pthread_mutex_unlock(&sync_handle->mutex);
    return success ? 0 : OS_TRANSPORT_WAIT_ERROR;
}

static uint32_t wait_for_task_complete(task_sync_t *sync_handle)
{
    return wait_for_task_complete_common(sync_handle, -1);
}

static uint32_t wait_for_task_complete_timeout(task_sync_t *sync_handle, int64_t timeout_ms)
{
    return wait_for_task_complete_common(sync_handle, timeout_ms);
}

static void task_sync_add_canceled_tasks(task_sync_t *sync, uint64_t canceled_tasks, bool timedout)
{
    bool should_free = false;

    if (!sync) {
        return;
    }

    pthread_mutex_lock(&sync->mutex);
    sync->request_canceled = 1;
    if (timedout) {
        sync->request_timedout = 1;
    }

    uint64_t accounted = sync->completed_tasks + sync->canceled_tasks;
    if (accounted < sync->total_tasks) {
        uint64_t remain = sync->total_tasks - accounted;
        sync->canceled_tasks += canceled_tasks > remain ? remain : canceled_tasks;
    }

    if (sync_all_tasks_accounted_locked(sync)) {
        sync->request_completed = 1;
        pthread_cond_signal(&sync->cond);
    }

    if (sync_should_free_locked(sync)) {
        sync->freeing = 1;
        should_free = true;
    }
    pthread_mutex_unlock(&sync->mutex);

    if (should_free) {
        free_sync_owned_resources(sync);
    }
}

static void mark_task_group_completed(task_sync_t *sync, bool task_success)
{
    bool should_free = false;

    if (!sync) {
        return;
    }

    pthread_mutex_lock(&sync->mutex);
    if (!task_success) {
        sync->request_canceled = 1;
        sync->request_completed = 1;
        OST_LOG_WARN("Task group marked completed early because one task failed.");
        pthread_cond_signal(&sync->cond);
    } else {
        sync->completed_tasks++;
        if (sync_all_tasks_accounted_locked(sync)) {
            OST_LOG_INFO("Task group completed successfully (completed=%lu, canceled=%lu, total_tasks=%lu).",
                         sync->completed_tasks,
                         sync->canceled_tasks,
                         sync->total_tasks);
            sync->request_completed = 1;
            pthread_cond_signal(&sync->cond);
        }
    }

    if (sync_should_free_locked(sync)) {
        sync->freeing = 1;
        should_free = true;
    }
    pthread_mutex_unlock(&sync->mutex);

    if (should_free) {
        free_sync_owned_resources(sync);
    }
}

// 更新jfc信息并绑定poll线程，确保poll线程能够正确识别和处理事件
static int32_t update_jfc_for_poll(urma_jfce_t *jfce, urma_jfc_t *jfc, bool urma_event_mode, ThreadPoolHandle pool)
{
    pool->urmaInfo.jfce = jfce;
    pool->urmaInfo.jfc = jfc;
    pool->urmaInfo.urma_event_mode = urma_event_mode;
    return 0;
}

static int validate_send_input(void *handle,
                               urma_jetty_info_t *jetty_info,
                               ost_buffer_info_t *local_src,
                               ost_buffer_info_t *remote_dst,
                               uint32_t len,
                               task_sync_t **ret_sync_handle)
{
    if (!handle || !jetty_info || !local_src || !remote_dst || !ret_sync_handle || len == 0) {
        OST_LOG_ERROR("Failed: invalid arguments "
                      "(handle=%p, jetty_info=%p, local_src=%p, remote_dst=%p, "
                      "ret_sync_handle=%p, len=%u)",
                      handle,
                      (void *)jetty_info,
                      (void *)local_src,
                      (void *)remote_dst,
                      (void *)ret_sync_handle,
                      len);
        return -1;
    }
    if (!g_inited) {
        OST_LOG_ERROR("Failed: os_transport is not initialized. "
                      "Call os_transport_init() before os_transport_send().");
        return -1;
    }
    return 0;
}

static int validate_recv_input(void *handle,
                               ost_buffer_info_t *host_src,
                               ost_device_info_t *device_dst,
                               uint32_t len,
                               task_sync_t **ret_sync_handle,
                               notify_callback_t notify_callback)
{
    if (!handle || !host_src || !device_dst || !ret_sync_handle || len == 0 || !notify_callback) {
        OST_LOG_ERROR("Failed: invalid arguments "
                      "(handle=%p, host_src=%p, device_dst=%p, ret_sync_handle=%p, len=%u, "
                      "notify_callback=%p)",
                      handle,
                      (void *)host_src,
                      (void *)device_dst,
                      (void *)ret_sync_handle,
                      len,
                      (void *)notify_callback);
        return -1;
    }
    if (!g_inited) {
        OST_LOG_ERROR("Failed: os_transport is not initialized. "
                      "Call os_transport_init() before os_transport_recv().");
        return -1;
    }
    return 0;
}

// 发送单个chunk时，构建对应的write_info。
static urma_write_info_t build_write_info(urma_jetty_info_t *jetty_info,
                                          ost_buffer_info_t *local_src,
                                          ost_buffer_info_t *remote_dst,
                                          uint32_t len,
                                          uint32_t server_key,
                                          uint32_t client_key)
{
    os_transport_user_data_t server_user_data = {
        .bs.request_id = server_key, .bs.chunk_type = MIDDLE_CHUNK, .bs.chunk_id = 0, .bs.chunk_size = len};
    os_transport_user_data_t client_user_data = {
        .bs.request_id = client_key, .bs.chunk_type = MIDDLE_CHUNK, .bs.chunk_id = 0, .bs.chunk_size = len};
    urma_write_info_t write_info = {.jfs = jetty_info->jfs,
                                    .jetty = jetty_info->jetty,
                                    .target_jfr = jetty_info->tjetty,
                                    .dst_tseg = remote_dst->tseg,
                                    .src_tseg = local_src->tseg,
                                    .flag.value = 0,
                                    .user_ctx_server = server_user_data,
                                    .user_ctx_client = client_user_data};
    return write_info;
}

static uint32_t common_split_chunks(
    uint64_t src_addr, uint64_t dst_addr, uint32_t len, chunk_info_t **ret_chunks, uint32_t *ret_chunk_num)
{
    uint32_t remain_len = len;
    uint32_t chunks_num;
    chunk_info_t *chunks;

    chunks_num = (remain_len + DEFAULT_CHUNK_SIZE - 1) / DEFAULT_CHUNK_SIZE;
    chunks = (chunk_info_t *)malloc(sizeof(chunk_info_t) * chunks_num);
    if (!chunks) {
        OST_LOG_ERROR("Failed: unable to allocate chunk array "
                      "(src_addr=0x%lx, dst_addr=0x%lx, len=%u, chunk_count=%u).",
                      src_addr,
                      dst_addr,
                      len,
                      chunks_num);
        return -1;
    }

    for (uint32_t i = 0; i < chunks_num; i++) {
        chunks[i].src = src_addr + i * DEFAULT_CHUNK_SIZE;
        chunks[i].dst = dst_addr + i * DEFAULT_CHUNK_SIZE;
        chunks[i].len = (remain_len - i * DEFAULT_CHUNK_SIZE) > DEFAULT_CHUNK_SIZE ?
                            DEFAULT_CHUNK_SIZE :
                            (remain_len - i * DEFAULT_CHUNK_SIZE);
    }
    *ret_chunks = chunks;
    *ret_chunk_num = chunks_num;
    return 0;
}

// 发送数据时切分chunk
static uint32_t send_split_chunks(ost_buffer_info_t *local_src,
                                  ost_buffer_info_t *remote_dst,
                                  uint32_t len,
                                  chunk_info_t **ret_chunks,
                                  uint32_t *ret_chunk_num)
{
    uint64_t src_addr;
    uint64_t dst_addr;

    if (!local_src || !remote_dst || len == 0) {
        OST_LOG_ERROR("Failed: invalid arguments "
                      "(local_src=%p, remote_dst=%p, len=%u).",
                      (void *)local_src,
                      (void *)remote_dst,
                      len);
        return -1;
    }
    src_addr = local_src->addr;
    dst_addr = remote_dst->addr;
    return common_split_chunks(src_addr, dst_addr, len, ret_chunks, ret_chunk_num);
}

// 接收数据时切分chunk
static uint32_t recv_split_chunks(ost_buffer_info_t *host,
                                  ost_device_info_t *device,
                                  uint32_t len,
                                  chunk_info_t **ret_chunks,
                                  uint32_t *ret_chunk_num)
{
    uint64_t src_addr;
    uint64_t dst_addr;

    if (!host || !device || len == 0) {
        OST_LOG_ERROR("Failed: invalid arguments (host=%p, device=%p, len=%u).", (void *)host, (void *)device, len);
        return -1;
    }
    src_addr = host->addr;
    dst_addr = (uint64_t)(uintptr_t)device->dst;
    return common_split_chunks(src_addr, dst_addr, len, ret_chunks, ret_chunk_num);
}

static void construct_send_task_arg(send_task_arg_t *arg,
                                    urma_write_info_t write_info,
                                    chunk_info_t *chunk_info,
                                    uint64_t chunk_id,
                                    bool is_last_chunk,
                                    task_sync_t *sync)
{
    // 显式构造每个位域字段，避免隐式保留旧值
    os_transport_user_data_t user_data_server = {0};
    os_transport_user_data_t user_data_client = {0};

    user_data_server.bs.request_id = write_info.user_ctx_server.bs.request_id; // 将server_key作为request_id传入
    user_data_server.bs.chunk_type = is_last_chunk ? LAST_CHUNK : MIDDLE_CHUNK;
    if (chunk_id == OS_TRANSPORT_MAX_CHUNK_ID) {
        OST_LOG_WARN("chunk_id=%lu exceeds user data bitfield range (max=%lu), value will be truncated.",
                     (unsigned long)chunk_id,
                     (unsigned long)OS_TRANSPORT_MAX_CHUNK_ID);
    }
    user_data_server.bs.chunk_id = chunk_id;
    user_data_server.bs.chunk_size = chunk_info->len;

    user_data_client.bs.request_id = write_info.user_ctx_client.bs.request_id; // 将client_key作为request_id传入
    user_data_client.bs.chunk_type = is_last_chunk ? LAST_CHUNK : MIDDLE_CHUNK;
    user_data_client.bs.chunk_id = chunk_id;
    user_data_client.bs.chunk_size = chunk_info->len;

    arg->write_info = write_info;
    arg->write_info.user_ctx_server = user_data_server;
    arg->write_info.user_ctx_client = user_data_client;
    arg->chunk_info = chunk_info;
    arg->is_last_chunk = is_last_chunk;

    // 同组所有task共享一个同步对象，便于主线程等待整组完成
    arg->sync = sync;
}

static void construct_recv_task_arg(recv_task_arg_t *arg,
                                    urma_recv_info_t recv_info,
                                    chunk_info_t *chunk_info,
                                    bool is_last_chunk,
                                    task_sync_t *sync,
                                    notify_callback_t notify_callback)
{
    memset(arg, 0, sizeof(*arg));
    arg->recv_info = recv_info;
    arg->chunk_info = chunk_info;
    arg->is_last_chunk = is_last_chunk;
    arg->sync = sync;
    arg->notify_callback = notify_callback;
}

// 构建供worker取用的task信息
static ThreadPoolTask construct_worker_task(
    uint64_t task_id, uint32_t request_id, int (*task_func)(void *), void *task_arg, TaskPrepareCb prepare_cb)
{
    ThreadPoolTask task;
    memset(&task, 0, sizeof(task));
    task.task_id = task_id;
    task.request_id = request_id;
    task.prepare_cb = prepare_cb;
    task.task_func = task_func;
    task.task_arg = task_arg;
    task.is_completed = false;
    task.free_task_self = false;
    return task;
}

static int do_send_chunk_for_worker(urma_write_info_t write_info, chunk_info_t *chunk_info)
{
    int ret = 0;
    ret = (int)urma_write_with_notify(write_info, chunk_info);
    if (ret != 0) {
        OST_LOG_ERROR("Failed: urma_write_with_notify returned %d for request_id=%u, chunk_id=%lu.",
                      ret,
                      write_info.user_ctx_client.bs.request_id,
                      write_info.user_ctx_client.bs.chunk_id);
    }
    return ret;
}

static void prepare_recv_task_user_data(void *task_arg, void *user_data)
{
    recv_task_arg_t *recv_task_arg = (recv_task_arg_t *)task_arg;
    os_transport_user_data_t *notify_user_data = (os_transport_user_data_t *)user_data;

    if (!recv_task_arg || !notify_user_data) {
        return;
    }

    recv_task_arg->notify_user_data = *notify_user_data;
}

// worker线程执行的send任务函数，负责发送chunk
static int send_task_worker_func(void *arg)
{
    int ret = 0;
    uint32_t request_id;
    uint32_t chunk_id;
    uint32_t chunk_len;
    task_sync_t *sync;

    if (!arg) {
        OST_LOG_ERROR("Failed: arg is NULL in send_task_worker_func.");
        return -1;
    }

    send_task_arg_t *send_task_arg = (send_task_arg_t *)arg;
    request_id = send_task_arg->write_info.user_ctx_client.bs.request_id;
    chunk_id = send_task_arg->write_info.user_ctx_client.bs.chunk_id;
    chunk_len = send_task_arg->chunk_info ? send_task_arg->chunk_info->len : 0;
    sync = send_task_arg->sync;

    ret = do_send_chunk_for_worker(send_task_arg->write_info, send_task_arg->chunk_info);
    if (ret != 0) {
        OST_LOG_WARN("Send worker task failed (request_id=%u, chunk_id=%u, len=%u).", request_id, chunk_id, chunk_len);
    }
    mark_task_group_completed(sync, ret == 0 ? true : false);
    return ret;
}

// worker线程执行的recv任务函数，负责H2D操作
static int recv_task_worker_func(void *arg)
{
    int ret = 0;
    uint32_t request_id;
    uint64_t chunk_id;
    uint64_t chunk_type;
    bool is_last_chunk;
    task_sync_t *sync;

    if (!arg) {
        OST_LOG_ERROR("Failed: arg is NULL in recv_task_worker_func.");
        return -1;
    }

    recv_task_arg_t *recv_task_arg = (recv_task_arg_t *)arg;
    request_id = recv_task_arg->recv_info.request_id;
    chunk_id = (uint64_t)recv_task_arg->notify_user_data.bs.chunk_id;
    chunk_type = (uint64_t)recv_task_arg->notify_user_data.bs.chunk_type;
    is_last_chunk = recv_task_arg->is_last_chunk;
    sync = recv_task_arg->sync;

    if (!recv_task_arg->notify_callback) {
        OST_LOG_ERROR("Failed: notify_callback is NULL in recv_task_worker_func "
                      "(request_id=%u).",
                      request_id);
        ret = -1;
    } else {
        ret = recv_task_arg->notify_callback(&recv_task_arg->notify_user_data);
    }
    if (ret != 0) {
        OST_LOG_WARN("Recv notify callback failed "
                     "(request_id=%u, chunk_id=%lu, chunk_type=%lu, ret=%d).",
                     request_id,
                     chunk_id,
                     chunk_type,
                     ret);
    }
    if (is_last_chunk) {
        // // 主线程返回后通过cudaEventSynchronize(event)等待所有h2d操作完成，确保数据可用
        // cudaStream_t stream = recv_task_arg->recv_info.device_info.stream;
        // cudaEvent_t event = recv_task_arg->recv_info.device_info.event;
        // int event_ret = cudaEventRecord(event, stream);
        // if (event_ret != 0) {
        //     OST_LOG_ERROR(
        //         "Failed: cudaEventRecord returned %d (request_id=%u).", event_ret,
        //         recv_task_arg->recv_info.request_id);
        // }
    }
    mark_task_group_completed(sync, ret == 0 ? true : false);
    return ret;
}

static int register_send_tasks(os_transport_handle_t *ost_handle,
                               chunk_info_t *chunks,
                               uint32_t chunk_num,
                               int (*task_func)(void *),
                               urma_info_t urma_info,
                               task_sync_t *sync)
{
    // 第0个chunk由调用线程发送，剩余chunk注册为task供worker线程发送，因此task_num为chunk_num-1
    uint32_t task_num = chunk_num - 1;
    uint64_t *task_ids = NULL;
    task_group_t *task_group = NULL;
    send_task_arg_t *task_args = NULL;

    if (chunk_num < 2) {
        OST_LOG_ERROR("Failed: chunk_num must be >= 2 for async send path "
                      "(chunk_num=%u).",
                      chunk_num);
        return -1;
    }

    if (alloc_task_group(&task_group, task_num, sizeof(send_task_arg_t)) != 0) {
        OST_LOG_ERROR("Failed: unable to allocate task group "
                      "(task_num=%u, arg_size=%zu).",
                      task_num,
                      sizeof(send_task_arg_t));
        return -1;
    }
    task_args = (send_task_arg_t *)task_group->task_args;

    sync->total_tasks = task_num;
    // 从第1个chunk开始注册task，第0个chunk由调用线程发送，确保task_id与chunk_id保持一致，便于追踪和调试
    for (uint32_t i = 0; i < task_num; i++) {
        uint32_t chunk_idx = i + 1;
        bool is_last_chunk = (chunk_idx == chunk_num - 1);
        uint32_t request_id = (uint32_t)(urma_info.write_info.user_ctx_server.bs.request_id);

        construct_send_task_arg(
            &task_args[i], urma_info.write_info, &chunks[chunk_idx], chunk_idx, is_last_chunk, sync);
        task_group->tasks[i] = construct_worker_task(chunk_idx, request_id, task_func, &task_args[i], NULL);
    }

    task_ids =
        thread_pool_submit_batch_tasks(ost_handle->thread_pool, task_group->tasks, task_num, NULL, NULL, NULL, NULL);
    if (!task_ids) {
        OST_LOG_ERROR("Failed: thread_pool_submit_batch_tasks returned NULL "
                      "(request_id=%u, task_num=%u).",
                      (uint32_t)(urma_info.write_info.user_ctx_server.bs.request_id),
                      task_num);
        free(task_group->task_args);
        free(task_group->tasks);
        free(task_group);
        return -1;
    }

    free(task_ids);
    sync->task_group = task_group;
    OST_LOG_INFO("Registered async send tasks (request_id=%u, task_num=%u, chunk_num=%u).",
                 (uint32_t)(urma_info.write_info.user_ctx_server.bs.request_id),
                 task_num,
                 chunk_num);
    return 0;
}

static int register_recv_tasks(os_transport_handle_t *ost_handle,
                               chunk_info_t *chunks,
                               uint32_t chunk_num,
                               int (*task_func)(void *),
                               urma_info_t urma_info,
                               task_sync_t *sync,
                               notify_callback_t notify_callback)
{
    uint64_t *task_ids = NULL;
    task_group_t *task_group = NULL;
    recv_task_arg_t *task_args = NULL;

    if (alloc_task_group(&task_group, chunk_num, sizeof(recv_task_arg_t)) != 0) {
        OST_LOG_ERROR("Failed: unable to allocate task group "
                      "(chunk_num=%u, arg_size=%zu).",
                      chunk_num,
                      sizeof(recv_task_arg_t));
        return -1;
    }
    task_args = (recv_task_arg_t *)task_group->task_args;

    sync->total_tasks = chunk_num;
    for (uint32_t i = 0; i < chunk_num; i++) {
        bool is_last_chunk = (i == chunk_num - 1);
        uint32_t request_id = (uint32_t)(urma_info.recv_info.request_id);
        construct_recv_task_arg(&task_args[i], urma_info.recv_info, &chunks[i], is_last_chunk, sync, notify_callback);
        task_group->tasks[i] =
            construct_worker_task(i, request_id, task_func, &task_args[i], prepare_recv_task_user_data);
    }

    task_ids =
        thread_pool_submit_batch_tasks(ost_handle->thread_pool, task_group->tasks, chunk_num, NULL, NULL, NULL, NULL);
    if (!task_ids) {
        OST_LOG_ERROR("Failed: thread_pool_submit_batch_tasks returned NULL "
                      "(request_id=%u, task_num=%u).",
                      (uint32_t)(urma_info.recv_info.request_id),
                      chunk_num);
        free(task_group->task_args);
        free(task_group->tasks);
        free(task_group);
        return -1;
    }

    free(task_ids);
    sync->task_group = task_group;
    OST_LOG_INFO("Registered async recv tasks (request_id=%u, task_num=%u).",
                 (uint32_t)(urma_info.recv_info.request_id),
                 chunk_num);
    return 0;
}

// 构造并注册所有task，sync_handle用于与主函数同步
static uint32_t construct_and_register_worker_task(os_transport_handle_t *ost_handle,
                                                   chunk_info_t *chunks,
                                                   uint32_t chunk_num,
                                                   task_type_t type,
                                                   int (*task_func)(void *),
                                                   urma_info_t urma_info,
                                                   task_sync_t **sync_handle,
                                                   notify_callback_t notify_callback)
{
    task_sync_t *sync = NULL;
    int ret = -1;

    if (!ost_handle || !chunks || !sync_handle || chunk_num == 0) {
        OST_LOG_ERROR("Failed: invalid arguments "
                      "(ost_handle=%p, chunks=%p, sync_handle=%p, chunk_num=%u, type=%d).",
                      (void *)ost_handle,
                      (void *)chunks,
                      (void *)sync_handle,
                      chunk_num,
                      type);
        return -1;
    }
    *sync_handle = NULL;

    if (init_task_sync(&sync) != 0) {
        OST_LOG_ERROR("Failed: init_task_sync returned error.");
        return -1;
    }

    if (type == SEND_TASK) {
        ret = register_send_tasks(ost_handle, chunks, chunk_num, task_func, urma_info, sync);
    } else if (type == RECV_TASK) {
        ret = register_recv_tasks(ost_handle, chunks, chunk_num, task_func, urma_info, sync, notify_callback);
    } else {
        OST_LOG_ERROR("Failed: unsupported task type (%d).", type);
        ret = -1;
    }

    if (ret != 0) {
        OST_LOG_ERROR("Failed: register worker task path returned error "
                      "(type=%d, chunk_num=%u).",
                      type,
                      chunk_num);
        pthread_mutex_destroy(&sync->mutex);
        pthread_cond_destroy(&sync->cond);
        free(sync);
        return -1;
    }

    *sync_handle = sync;
    return 0;
}

static int register_tasks_and_bind_chunks(os_transport_handle_t *ost_handle,
                                          chunk_info_t *chunks,
                                          uint32_t chunk_num,
                                          task_type_t type,
                                          int (*task_func)(void *),
                                          urma_info_t urma_info,
                                          task_sync_t **sync_handle,
                                          notify_callback_t notify_callback)
{
    task_sync_t *sync = NULL;
    int ret;

    if (!sync_handle) {
        OST_LOG_ERROR("Failed: sync_handle is NULL in register_tasks_and_bind_chunks.");
        return -1;
    }

    ret = construct_and_register_worker_task(
        ost_handle, chunks, chunk_num, type, task_func, urma_info, &sync, notify_callback);
    if (ret != 0) {
        return -1;
    }

    sync->chunks = chunks;
    *sync_handle = sync;
    return 0;
}

static int send_single_chunk(urma_jetty_info_t *jetty_info,
                             ost_buffer_info_t *local_src,
                             ost_buffer_info_t *remote_dst,
                             uint32_t len,
                             uint32_t server_key,
                             uint32_t client_key)
{
    urma_write_info_t write_info = build_write_info(jetty_info, local_src, remote_dst, len, server_key, client_key);
    write_info.user_ctx_server.bs.chunk_type = LAST_CHUNK;
    write_info.user_ctx_client.bs.chunk_type = LAST_CHUNK;
    chunk_info_t chunk = {.src = local_src[0].addr, .dst = remote_dst[0].addr, .len = len};
    if (urma_write_with_notify(write_info, &chunk) != URMA_SUCCESS) {
        OST_LOG_ERROR("Failed: URMA write_with_notify returned failure "
                      "(len=%u, server_key=%u, client_key=%u, src=0x%lx, dst=0x%lx).",
                      len,
                      server_key,
                      client_key,
                      chunk.src,
                      chunk.dst);
        return -1;
    }
    return 0;
}

uint32_t os_transport_reg_jfc(urma_jfce_t *jfce, urma_jfc_t *jfc, void *handle)
{
    os_transport_handle_t *ost_handle;

    if (!g_inited) {
        OST_LOG_ERROR("Failed: os_transport is not initialized.");
        return -1;
    }
    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL.");
        return -1;
    }

    ost_handle = (os_transport_handle_t *)handle;
    // 初始化完成，poll线程已拉起，更新jfc，绑定poll
    if (update_jfc_for_poll(jfce, jfc, ost_handle->urma_event_mode, ost_handle->thread_pool) != 0) {
        OST_LOG_ERROR("Failed: update_jfc_for_poll returned error "
                      "(jfce=%p, jfc=%p, event_mode=%d, thread_pool=%p).",
                      (void *)jfce,
                      (void *)jfc,
                      (int)ost_handle->urma_event_mode,
                      (void *)ost_handle->thread_pool);
        return -1;
    }

    OST_LOG_INFO("Succeeded: JFC/JFCE registered to thread-pool poller "
                 "(jfce=%p, jfc=%p, event_mode=%d).",
                 (void *)jfce,
                 (void *)jfc,
                 (int)ost_handle->urma_event_mode);
    return 0;
}

uint32_t os_transport_init(urma_context_t *urma_ctx, os_transport_cfg_t *ost_cfg, void **handle)
{
    os_transport_handle_t *ost_handle = NULL;

    OST_LOG_INFO("Initializing os_transport (urma_ctx=%p, ost_cfg=%p, handle=%p).",
                 (void *)urma_ctx,
                 (void *)ost_cfg,
                 (void *)handle);

    if (!ost_cfg || !handle) {
        OST_LOG_ERROR("Failed: invalid arguments (ost_cfg=%p, handle=%p).", (void *)ost_cfg, (void *)handle);
        return -1;
    }
    *handle = NULL;
    if (g_inited) {
        OST_LOG_ERROR("Failed: os_transport is already initialized.");
        return -1;
    }

    ost_handle = malloc(sizeof(os_transport_handle_t));
    if (!ost_handle) {
        OST_LOG_ERROR("Failed: unable to allocate os_transport_handle_t "
                      "(size=%zu).",
                      sizeof(os_transport_handle_t));
        return -1;
    }
    memset(ost_handle, 0, sizeof(os_transport_handle_t));

    ost_handle->urma_ctx = urma_ctx;
    ost_handle->worker_thread_num = ost_cfg->worker_thread_num;
    ost_handle->urma_event_mode = ost_cfg->urma_event_mode;

    if (init_recv_queue_limiter(ost_handle, ost_cfg->recv_queue_capacity) != 0) {
        OST_LOG_ERROR("Failed: init_recv_queue_limiter returned error.");
        free(ost_handle);
        return -1;
    }

    g_inited = 1;

    // 初始化线程池
    // worker_thread_num: Worker线程数量; pending_queue_cap: 0表示使用默认值1024
    ost_handle->thread_pool = thread_pool_init(ost_cfg->worker_thread_num, 0);
    if (!ost_handle->thread_pool) {
        if (ost_cfg->worker_thread_num == 0) {
            OST_LOG_WARN("os_transport initialized without worker threads because worker_thread_num is 0.");
            g_inited = 0;
            destroy_recv_queue_limiter(ost_handle);
            free(ost_handle);
            return 0;
        }
        OST_LOG_ERROR("Failed: thread_pool_init returned NULL "
                      "(worker_thread_num=%u).",
                      ost_cfg->worker_thread_num);
        goto init_fail;
    }

    // 先注册jfc信息，确保线程池中poll线程能够正确识别和处理事件，后续send/recv接口调用时无需重复注册
    if (os_transport_reg_jfc(ost_cfg->jfce, ost_cfg->jfc, (void *)ost_handle) != 0) {
        OST_LOG_ERROR("Failed: os_transport_reg_jfc returned error "
                      "(jfce=%p, jfc=%p).",
                      (void *)ost_cfg->jfce,
                      (void *)ost_cfg->jfc);
        goto init_fail;
    }

    if (thread_pool_start(ost_handle->thread_pool) != 0) {
        OST_LOG_ERROR("Failed: thread_pool_start returned error.");
        goto destroy_thread_pool;
    }

    *handle = (void *)ost_handle;
    OST_LOG_INFO("Succeeded: handle=%p, worker_thread_num=%u, event_mode=%d.",
                 (void *)ost_handle,
                 ost_handle->worker_thread_num,
                 (int)ost_handle->urma_event_mode);
    return 0;

destroy_thread_pool:
    thread_pool_destroy(ost_handle->thread_pool);
    ost_handle->thread_pool = NULL;
init_fail:
    g_inited = 0;
    destroy_recv_queue_limiter(ost_handle);
    free(ost_handle);
    return -1;
}

/*
 * 发送数据的函数实现
 * 1. 如果数据长度小于等于DEFAULT_CHUNK_SIZE，则直接发送；
 * 2. 如果数据长度大于DEFAULT_CHUNK_SIZE，则拆分为多个chunk，每个chunk的大小不超过DEFAULT_CHUNK_SIZE
 * 3.
 * 将剩余chunk注册为对应task，最后一个chunk使用的回调函数负责唤醒os_transport_send的线程继续执行。
 * 4. 手动发送第一个chunk，触发notify机制，后续chunk的发送由对应的worker线程完成。
 * 5. os_transport_send的线程等待所有chunk发送完成后返回。
 */
uint32_t os_transport_send(void *handle,
                           urma_jetty_info_t *jetty_info,
                           ost_buffer_info_t *local_src,
                           ost_buffer_info_t *remote_dst,
                           uint32_t len,
                           uint32_t server_key,
                           uint32_t client_key,
                           task_sync_t **ret_sync_handle)
{
    urma_write_info_t write_info;
    urma_info_t urma_info;
    os_transport_handle_t *ost_handle = (os_transport_handle_t *)handle;
    chunk_info_t *chunks;
    uint32_t chunks_num;
    task_sync_t *sync_handle = NULL;
    uint32_t ret = -1;

    if (ret_sync_handle) {
        *ret_sync_handle = NULL;
    }

    OST_LOG_INFO("Submitting send request (len=%u, server_key=%u, client_key=%u).", len, server_key, client_key);

    if (validate_send_input(handle, jetty_info, local_src, remote_dst, len, ret_sync_handle) != 0) {
        return ret;
    }

    if (len <= DEFAULT_CHUNK_SIZE) {
        OST_LOG_INFO(
            "Using single-chunk send path (len=%u, server_key=%u, client_key=%u).", len, server_key, client_key);
        return send_single_chunk(jetty_info, local_src, remote_dst, len, server_key, client_key);
    }

    if (send_split_chunks(local_src, remote_dst, len, &chunks, &chunks_num) != 0) {
        return ret;
    }

    write_info = build_write_info(jetty_info, local_src, remote_dst, chunks[0].len, server_key, client_key);
    memset(&urma_info, 0, sizeof(urma_info));
    urma_info.write_info = write_info;

    if (register_tasks_and_bind_chunks(
            ost_handle, chunks, chunks_num, SEND_TASK, send_task_worker_func, urma_info, &sync_handle, NULL)
        != 0) {
        OST_LOG_ERROR("Failed: unable to register async send tasks "
                      "(server_key=%u, client_key=%u, chunk_count=%u).",
                      server_key,
                      client_key,
                      chunks_num);
        free(chunks);
        return ret;
    }
    *ret_sync_handle = sync_handle;
    OST_LOG_INFO("Async send request registered successfully "
                 "(server_key=%u, client_key=%u, chunk_count=%u).",
                 server_key,
                 client_key,
                 chunks_num);
    if (urma_write_with_notify(write_info, &chunks[0]) != URMA_SUCCESS) {
        OST_LOG_ERROR("Failed: first chunk submission returned URMA error "
                      "(total_len=%u, chunk_count=%u, server_key=%u, client_key=%u).",
                      len,
                      chunks_num,
                      server_key,
                      client_key);
        // 如果第一个chunk发送失败，应该直接标记整个请求完成，唤醒等待线程，并不要求后续task执行完成，避免死锁
        pthread_mutex_lock(&sync_handle->mutex);
        sync_handle->request_completed = 1;
        pthread_cond_signal(&sync_handle->cond);
        pthread_mutex_unlock(&sync_handle->mutex);
        return -1;
    }

    return 0;
}

uint32_t os_transport_recv(void *handle,
                           ost_buffer_info_t *host_src,
                           ost_device_info_t *device_dst,
                           uint32_t len,
                           uint32_t client_key,
                           task_sync_t **ret_sync_handle,
                           notify_callback_t notify_callback)
{
    urma_info_t urma_info = {0};
    os_transport_handle_t *ost_handle = (os_transport_handle_t *)handle;
    chunk_info_t *chunks;
    uint32_t chunks_num;
    uint32_t reused_recv_queue_count = 0;
    uint32_t post_recv_queue_count = 0;
    task_sync_t *sync_handle = NULL;

    if (ret_sync_handle) {
        *ret_sync_handle = NULL;
    }

    OST_LOG_INFO("Submitting recv request (len=%u, client_key=%u).", len, client_key);

    if (validate_recv_input(handle, host_src, device_dst, len, ret_sync_handle, notify_callback) != 0) {
        return -1;
    }

    if (recv_split_chunks(host_src, device_dst, len, &chunks, &chunks_num) != 0) {
        return -1;
    }

    urma_info.recv_info = (urma_recv_info_t){
        .jfr = device_dst->jfr, .local_tseg = host_src->tseg, .device_info = *device_dst, .request_id = client_key};

    if (acquire_recv_queue_resources(ost_handle, chunks_num, &reused_recv_queue_count, &post_recv_queue_count) != 0) {
        OST_LOG_ERROR("Failed: unable to acquire recv queue resources "
                      "(len=%u, chunk_count=%u, client_key=%u).",
                      len,
                      chunks_num,
                      client_key);
        free(chunks);
        return -1;
    }
    OST_LOG_INFO("Recv queue resources prepared (client_key=%u, chunk_count=%u, reused=%u, need_post=%u).",
                 client_key,
                 chunks_num,
                 reused_recv_queue_count,
                 post_recv_queue_count);

    if (register_tasks_and_bind_chunks(
            ost_handle, chunks, chunks_num, RECV_TASK, recv_task_worker_func, urma_info, &sync_handle, notify_callback)
        != 0) {
        OST_LOG_ERROR("Failed: unable to register async recv tasks "
                      "(client_key=%u, chunk_count=%u).",
                      client_key,
                      chunks_num);
        release_recv_queue_resources(ost_handle, post_recv_queue_count, reused_recv_queue_count);
        free(chunks);
        return -1;
    }

    OST_LOG_INFO("Async recv request registered successfully (client_key=%u, chunk_count=%u).", client_key, chunks_num);
    for (uint32_t i = reused_recv_queue_count; i < chunks_num; i++) {
        if (urma_recv_with_notify(urma_info.recv_info, &chunks[i]) != URMA_SUCCESS) {
            uint32_t successful_post_count = i - reused_recv_queue_count;
            uint32_t unposted_count = post_recv_queue_count - successful_post_count;
            release_recv_queue_resources(ost_handle, unposted_count, reused_recv_queue_count + successful_post_count);
            OST_LOG_ERROR("Failed: urma_recv_with_notify returned URMA error "
                          "(len=%u, chunk_count=%u, client_key=%u, reused=%u, posted=%u, remaining=%u).",
                          len,
                          chunks_num,
                          client_key,
                          reused_recv_queue_count,
                          successful_post_count,
                          unposted_count);
            // 如果recv提交失败，应该直接标记整个请求完成，唤醒等待线程，并不要求后续task执行完成，避免死锁
            pthread_mutex_lock(&sync_handle->mutex);
            sync_handle->request_completed = 1;
            pthread_cond_signal(&sync_handle->cond);
            pthread_mutex_unlock(&sync_handle->mutex);
            return -1;
        }
    }
    *ret_sync_handle = sync_handle;

    return 0;
}

int os_transport_wake_up_task(void *handle, void *cr_t)
{
    int ret;

    if (!handle || !cr_t) {
        OST_LOG_ERROR("Failed: invalid arguments in os_transport_wake_up_task (handle=%p, cr_t=%p).", handle, cr_t);
        return -1;
    }

    os_transport_handle_t *ost_handle = (os_transport_handle_t *)handle;
    urma_cr_t *cr = (urma_cr_t *)cr_t;
    ThreadPoolHandle pool = ost_handle->thread_pool;
    TransportData user_data = {0};
    urma_cr_opcode_t opcode = cr->opcode;
    if (opcode == URMA_CR_OPC_WRITE_WITH_IMM) {
        user_data = (TransportData)cr->imm_data;
    } else if (opcode == URMA_CR_OPC_SEND) {
        user_data = (TransportData)cr->user_ctx;
    } else {
        OST_LOG_ERROR("Unknown opcode %d", opcode);
        return -1;
    }
    uint32_t request_id = user_data.bs.request_id;

    /*
     * 线程池会保存本次completion的user_data副本；后续recv worker再把对应副本的
     * 指针传入notify_callback，避免传递当前栈变量地址。
     */
    ret = thread_pool_wake_up_worker_by_req_id(pool, request_id, &user_data);
    if (opcode == URMA_CR_OPC_WRITE_WITH_IMM) {
        release_recv_queue_resources(ost_handle, 1, 0);
        OST_LOG_INFO("Recv queue resource released after completion (request_id=%u).", request_id);
    }
    if (ret != 0) {
        OST_LOG_WARN("Failed to wake worker for completion event "
                     "(request_id=%u, opcode=%d).",
                     request_id,
                     opcode);
    }

    return ret;
}

static uint32_t wait_and_free_sync_common(void *handle, task_sync_t *sync_handle, int64_t timeout_ms)
{
    uint32_t wait_ret = 0;
    uint32_t request_id;
    os_transport_handle_t *ost_handle = (os_transport_handle_t *)handle;
    task_group_t *task_group;

    if (!ost_handle || !sync_handle) {
        OST_LOG_ERROR("Failed: invalid arguments (handle=%p, sync_handle=%p).", handle, (void *)sync_handle);
        return OS_TRANSPORT_WAIT_ERROR;
    }
    task_group = sync_handle->task_group;
    if (!task_group || !task_group->tasks || task_group->task_num == 0) {
        OST_LOG_ERROR("Failed: invalid task_group in sync_handle (sync_handle=%p, task_group=%p).",
                      (void *)sync_handle,
                      (void *)task_group);
        sync_request_release(sync_handle);
        return OS_TRANSPORT_WAIT_ERROR;
    }

    request_id = task_group->tasks[0].request_id;

    OST_LOG_INFO("Waiting for request completion (request_id=%u, total_tasks=%lu, timeout_ms=%ld).",
                 request_id,
                 sync_handle->total_tasks,
                 (long)timeout_ms);

    if (timeout_ms < 0) {
        wait_ret = wait_for_task_complete(sync_handle);
    } else {
        wait_ret = wait_for_task_complete_timeout(sync_handle, timeout_ms);
    }

    if (wait_ret != 0) {
        int canceled = thread_pool_cancel_tasks_by_req(ost_handle->thread_pool, request_id);
        if (canceled < 0) {
            canceled = 0;
        }
        task_sync_add_canceled_tasks(sync_handle, (uint64_t)canceled, wait_ret == OS_TRANSPORT_WAIT_TIMEOUT);
        OST_LOG_WARN("Request wait did not complete successfully; canceled queued tasks "
                     "(request_id=%u, ret=%u, canceled=%d, timeout_ms=%ld).",
                     request_id,
                     wait_ret,
                     canceled,
                     (long)timeout_ms);
    }

    pthread_mutex_lock(&sync_handle->mutex);
    sync_handle->release_requested = 1;
    bool should_free = sync_should_free_locked(sync_handle);
    if (should_free) {
        sync_handle->freeing = 1;
    }
    uint64_t completed = sync_handle->completed_tasks;
    uint64_t canceled = sync_handle->canceled_tasks;
    uint64_t total = sync_handle->total_tasks;
    pthread_mutex_unlock(&sync_handle->mutex);

    if (should_free) {
        free_sync_owned_resources(sync_handle);
    }

    if (wait_ret == 0) {
        OST_LOG_INFO("Request completed and resources released successfully (request_id=%u).", request_id);
    } else {
        OST_LOG_WARN("Request finished with non-success status; release requested "
                     "(request_id=%u, completed=%lu, canceled=%lu, total=%lu, ret=%u).",
                     request_id,
                     completed,
                     canceled,
                     total,
                     wait_ret);
    }
    return wait_ret;
}

uint32_t wait_and_free_sync(void *handle, task_sync_t *sync_handle)
{
    return wait_and_free_sync_common(handle, sync_handle, -1);
}

uint32_t wait_and_free_sync_timeout(void *handle, task_sync_t *sync_handle, int64_t timeout_ms)
{
    return wait_and_free_sync_common(handle, sync_handle, timeout_ms);
}

uint32_t os_transport_cancel_tasks(void *handle, task_sync_t **sync_handle, uint32_t request_id)
{
    os_transport_handle_t *ost_handle = (os_transport_handle_t *)handle;
    task_sync_t *sync = NULL;

    if (!ost_handle) {
        OST_LOG_ERROR("Failed: handle is NULL.");
        return -1;
    }
    if (!g_inited) {
        OST_LOG_ERROR("Failed: os_transport is not initialized.");
        return -1;
    }

    int ret = thread_pool_cancel_tasks_by_req(ost_handle->thread_pool, request_id);
    if (ret < 0) {
        OST_LOG_WARN("Failed to cancel tasks for request_id=%u.", request_id);
        return -1;
    }

    if (sync_handle && *sync_handle) {
        sync = *sync_handle;
        *sync_handle = NULL;
        task_sync_add_canceled_tasks(sync, (uint64_t)ret, false);
        pthread_mutex_lock(&sync->mutex);
        sync->release_requested = 1;
        bool should_free = sync_should_free_locked(sync);
        if (should_free) {
            sync->freeing = 1;
        }
        pthread_mutex_unlock(&sync->mutex);
        if (should_free) {
            free_sync_owned_resources(sync);
        }
    }

    OST_LOG_INFO("Tasks cancelled successfully for request_id=%u (removed=%d).", request_id, ret);
    return 0;
}

uint32_t os_transport_destroy(void *handle)
{
    os_transport_handle_t *ost_handle;

    OST_LOG_INFO("Destroying os_transport handle (handle=%p).", handle);

    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL.");
        return -1;
    }
    ost_handle = (os_transport_handle_t *)handle;
    if (!g_inited) {
        OST_LOG_ERROR("Failed: os_transport is not initialized.");
        return -1;
    }

    // 销毁线程池
    if (ost_handle->thread_pool) {
        thread_pool_destroy(ost_handle->thread_pool);
        ost_handle->thread_pool = NULL;
    }
    destroy_recv_queue_limiter(ost_handle);

    g_inited = 0;
    OST_LOG_INFO("Succeeded: resources released and thread pool stopped "
                 "(handle=%p).",
                 (void *)ost_handle);
    free(ost_handle);
    return 0;
}

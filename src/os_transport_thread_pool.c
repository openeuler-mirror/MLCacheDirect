#include "os_transport_thread_pool.h"
#include "os_transport_log_internal.h"
#include "os_transport_thread_pool_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/prctl.h>

// #define TEST_MODE  // 由编译选项定义

#ifdef TEST_MODE
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>

// 模拟事件队列（用于测试）
typedef struct {
    uint64_t *events;
    uint32_t cap;
    uint32_t head;
    uint32_t tail;
    uint32_t size;
    pthread_mutex_t lock;
    pthread_cond_t cond;
} MockEventQueue;

static MockEventQueue g_mock_queue = {0};

void mock_event_queue_init(uint32_t cap)
{
    g_mock_queue.events = malloc(cap * sizeof(uint64_t));
    g_mock_queue.cap = cap;
    g_mock_queue.head = g_mock_queue.tail = g_mock_queue.size = 0;
    pthread_mutex_init(&g_mock_queue.lock, NULL);
    pthread_cond_init(&g_mock_queue.cond, NULL);
}

void mock_event_queue_push(uint64_t req_id)
{
    pthread_mutex_lock(&g_mock_queue.lock);
    if (g_mock_queue.size >= g_mock_queue.cap) {
        uint32_t new_cap = g_mock_queue.cap * 2;
        uint64_t *new_events = malloc(new_cap * sizeof(uint64_t));
        for (uint32_t i = 0; i < g_mock_queue.size; i++) {
            new_events[i] = g_mock_queue.events[(g_mock_queue.head + i) % g_mock_queue.cap];
        }
        free(g_mock_queue.events);
        g_mock_queue.events = new_events;
        g_mock_queue.cap = new_cap;
        g_mock_queue.head = 0;
        g_mock_queue.tail = g_mock_queue.size;
    }
    g_mock_queue.events[g_mock_queue.tail] = req_id;
    g_mock_queue.tail = (g_mock_queue.tail + 1) % g_mock_queue.cap;
    g_mock_queue.size++;
    pthread_cond_signal(&g_mock_queue.cond);
    pthread_mutex_unlock(&g_mock_queue.lock);
}

static int mock_event_queue_pop(uint64_t *req_id)
{
    pthread_mutex_lock(&g_mock_queue.lock);
    if (g_mock_queue.size == 0) {
        pthread_mutex_unlock(&g_mock_queue.lock);
        return 0;
    }
    *req_id = g_mock_queue.events[g_mock_queue.head];
    g_mock_queue.head = (g_mock_queue.head + 1) % g_mock_queue.cap;
    g_mock_queue.size--;
    pthread_mutex_unlock(&g_mock_queue.lock);
    return 1;
}

void mock_event_queue_destroy(void)
{
    free(g_mock_queue.events);
    pthread_mutex_destroy(&g_mock_queue.lock);
    pthread_cond_destroy(&g_mock_queue.cond);
}
#endif

// 哈希函数
static uint32_t hash_req_id(uint32_t req_id)
{
    return (uint32_t)(req_id ^ (req_id >> 20)) % REQ_HASH_SIZE;
}

// 内部任务包装
typedef struct {
    int (*user_func)(void *);
    void *user_arg;
    TaskPrepareCb prepare_cb;
    TaskCompleteCb complete_cb;
    void *user_data;
    uint64_t task_id;
    uint32_t request_id;
    bool success;
} InternalTask;

// 任务包装函数
static int internal_task_wrapper(void *arg)
{
    InternalTask *itask = (InternalTask *)arg;

    if (!itask || !itask->user_func) {
        OST_LOG_ERROR("Failed: invalid internal task wrapper arguments (itask=%p).", arg);
        return -1;
    }

    int ret = itask->user_func(itask->user_arg);
    itask->success = (ret == 0);
    if (itask->complete_cb) {
        itask->complete_cb(itask->task_id, itask->success, itask->user_data);
    }
    if (ret != 0) {
        OST_LOG_WARN(
            "Worker task returned error (task_id=%lu, request_id=%u, ret=%d).", itask->task_id, itask->request_id, ret);
    }
    OST_LOG_INFO("Taskid = %lu request_id=%u completed", itask->task_id);
    free(itask);
    return ret;
}

static void prepare_internal_task_user_data(ThreadPoolTask *task, void *user_data)
{
    InternalTask *itask;

    if (!task || !task->task_arg || !user_data) {
        return;
    }

    itask = (InternalTask *)task->task_arg;
    if (itask->prepare_cb) {
        itask->prepare_cb(itask->user_arg, user_data);
    }
}

// 生成唯一任务ID
static uint64_t generate_task_id(ThreadPoolHandle pool)
{
    uint64_t id;
    pthread_mutex_lock(&pool->task_id_mutex);
    id = pool->next_task_id++;
    pthread_mutex_unlock(&pool->task_id_mutex);
    return id;
}

// 向 worker 队列添加任务（必须已持有 worker->mutex）
static bool worker_queue_push(WorkerThread *worker, ThreadPoolTask *task)
{
    TaskNode *node = malloc(sizeof(TaskNode));
    if (!node) {
        OST_LOG_ERROR("Failed: unable to allocate TaskNode for worker %d.", worker ? worker->worker_idx : -1);
        return false;
    }
    node->task = task;
    node->next = NULL;

    if (worker->queue_tail) {
        worker->queue_tail->next = node;
    } else {
        worker->queue_head = node;
    }
    worker->queue_tail = node;
    worker->queue_size++;
    return true;
}

// 从 worker 队列中取出指定 request_id 的第一个任务（必须已持有 worker->mutex）
static ThreadPoolTask *worker_queue_pop_by_req(WorkerThread *worker, uint32_t req_id)
{
    TaskNode *prev = NULL;
    TaskNode *curr = worker->queue_head;
    while (curr) {
        if (curr->task->request_id == req_id) {
            ThreadPoolTask *task = curr->task;
            if (prev) {
                prev->next = curr->next;
            } else {
                worker->queue_head = curr->next;
            }
            if (curr == worker->queue_tail) {
                worker->queue_tail = prev;
            }
            worker->queue_size--;
            free(curr);
            return task;
        }
        prev = curr;
        curr = curr->next;
    }
    return NULL;
}

// 向 worker 的待执行队列追加 request_id 以及对应completion user_data（必须已持有 worker->mutex）
static bool worker_pending_req_push(WorkerThread *worker, uint32_t req_id, void *user_data)
{
    PendingReqNode *node = malloc(sizeof(PendingReqNode));
    if (!node) {
        OST_LOG_ERROR("Failed: unable to allocate PendingReqNode for worker %d, request_id=%u.",
                      worker ? worker->worker_idx : -1,
                      req_id);
        return false;
    }

    node->request_id = req_id;
    memset(&node->user_data, 0, sizeof(node->user_data));
    if (user_data) {
        node->user_data = *(TransportData *)user_data;
    }
    node->next = NULL;

    if (worker->pending_req_tail) {
        worker->pending_req_tail->next = node;
    } else {
        worker->pending_req_head = node;
    }
    worker->pending_req_tail = node;
    worker->pending_req_count++;
    return true;
}

// 从 worker 的待执行队列取出 request_id 以及对应completion user_data（必须已持有 worker->mutex）
static bool worker_pending_req_pop(WorkerThread *worker, uint32_t *req_id, TransportData *user_data)
{
    PendingReqNode *node = worker->pending_req_head;
    if (!node)
        return false;

    *req_id = node->request_id;
    if (user_data) {
        *user_data = node->user_data;
    }
    worker->pending_req_head = node->next;
    if (!worker->pending_req_head) {
        worker->pending_req_tail = NULL;
    }
    worker->pending_req_count--;
    free(node);
    return true;
}

// 清理 worker 的待执行 request 队列（必须已持有 worker->mutex）
static void worker_pending_req_clear(WorkerThread *worker)
{
    PendingReqNode *curr = worker->pending_req_head;
    while (curr) {
        PendingReqNode *next = curr->next;
        free(curr);
        curr = next;
    }

    worker->pending_req_head = NULL;
    worker->pending_req_tail = NULL;
    worker->pending_req_count = 0;
}

// 从 worker 的待执行 request 队列中删除指定 request_id（必须已持有 worker->mutex）
static uint32_t worker_pending_req_remove_by_req(WorkerThread *worker, uint32_t req_id)
{
    PendingReqNode *prev = NULL;
    PendingReqNode *curr = worker->pending_req_head;
    uint32_t removed = 0;

    while (curr) {
        if (curr->request_id == req_id) {
            PendingReqNode *to_free = curr;
            if (prev) {
                prev->next = curr->next;
            } else {
                worker->pending_req_head = curr->next;
            }
            curr = curr->next;
            if (to_free == worker->pending_req_tail) {
                worker->pending_req_tail = prev;
            }
            free(to_free);
            removed++;
        } else {
            prev = curr;
            curr = curr->next;
        }
    }

    worker->pending_req_count -= removed;
    return removed;
}

// 查找最佳 worker：按综合负载选择最轻的 worker
static WorkerThread *select_best_worker(ThreadPoolHandle pool)
{
    WorkerThread *best = NULL;
    uint32_t best_load = UINT32_MAX;
    uint32_t best_busy_bias = UINT32_MAX;

    for (uint32_t i = 0; i < pool->worker_count; i++) {
        WorkerThread *w = &pool->workers[i];
        uint32_t load;
        uint32_t busy_bias;

        pthread_mutex_lock(&w->mutex);

        // queue_size 表示该 worker 尚未消费的任务数；
        // pending_req_count 表示 completion 已到达、即将真正执行的 request 数。
        // 在高并发下，这两部分都代表这个 worker 的未来负载。
        load = w->queue_size + w->pending_req_count;
        busy_bias = (w->state == WORKER_STATE_BUSY) ? 1U : 0U;

        // 负载为 0 且当前不在执行任务，说明这是当前最理想的 worker，直接返回。
        if (load == 0 && busy_bias == 0) {
            best = w;
            pthread_mutex_unlock(&w->mutex);
            break;
        }

        // 优先选择综合负载更小的 worker；
        // 负载相同时，优先选择当前不是 BUSY 的 worker，减少把新请求继续压到正在执行的线程上。
        if (!best || load < best_load || (load == best_load && busy_bias < best_busy_bias)) {
            best = w;
            best_load = load;
            best_busy_bias = busy_bias;
        }

        pthread_mutex_unlock(&w->mutex);
    }

    return best;
}

// 哈希表操作
static RequestContext *find_req_context_locked(ThreadPoolHandle pool, uint32_t req_id)
{
    uint32_t h = hash_req_id(req_id);
    RequestContext *ctx = pool->req_hash[h];
    while (ctx) {
        if (ctx->request_id == req_id) {
            break;
        }
        ctx = ctx->next;
    }
    return ctx;
}

static void insert_req_context_locked(ThreadPoolHandle pool, RequestContext *ctx)
{
    uint32_t h = hash_req_id(ctx->request_id);
    ctx->next = pool->req_hash[h];
    pool->req_hash[h] = ctx;
}

static bool remove_req_context_locked(ThreadPoolHandle pool, uint32_t req_id)
{
    uint32_t h = hash_req_id(req_id);
    RequestContext **p = &pool->req_hash[h];
    while (*p) {
        if ((*p)->request_id == req_id) {
            RequestContext *tmp = *p;
            *p = tmp->next;
            free(tmp);
            return true;
        }
        p = &(*p)->next;
    }
    return false;
}

// worker 执行任务并处理计数
static void worker_process_task(WorkerThread *worker, ThreadPoolTask *task, uint32_t req_id, TransportData *user_data)
{
    ThreadPoolHandle pool = worker->pool;
    int ret;

    if (!task || !task->task_func) {
        OST_LOG_ERROR("Failed: invalid task for worker_process_task (worker=%d, request_id=%u, task=%p).",
                      worker ? worker->worker_idx : -1,
                      req_id,
                      (void *)task);
        return;
    }

    prepare_internal_task_user_data(task, user_data);
    ret = task->task_func(task->task_arg);
    task->is_completed = (ret == 0);

    pthread_mutex_lock(&pool->req_hash_mutex);
    RequestContext *ctx = find_req_context_locked(pool, req_id);
    if (ctx) {
        TaskCompleteCb batch_cb = NULL;
        void *batch_data = NULL;

        ctx->pending_count--;
        if (ctx->pending_count == 0) {
            batch_cb = ctx->batch_cb;
            batch_data = ctx->batch_user_data;
            (void)remove_req_context_locked(pool, req_id);
        }
        pthread_mutex_unlock(&pool->req_hash_mutex);

        if (batch_cb) {
            batch_cb(0, true, batch_data);
            OST_LOG_INFO("Request batch completed (request_id=%u, worker=%d).", req_id, worker->worker_idx);
        }
    } else {
        pthread_mutex_unlock(&pool->req_hash_mutex);
        OST_LOG_WARN(
            "No request context found after task execution (request_id=%u, worker=%d).", req_id, worker->worker_idx);
    }

    if (ret != 0) {
        OST_LOG_WARN("Task execution failed in worker_process_task (request_id=%u, worker=%d, ret=%d).",
                     req_id,
                     worker->worker_idx,
                     ret);
    }

    free(task);
}

static void set_worker_thread_name(int idx)
{
    char name[16] = {0};
    (void)snprintf(name, sizeof(name), "ost_wkr_%d", idx);
    (void)prctl(PR_SET_NAME, name, 0, 0, 0);
}

// worker 线程主函数
static void *worker_routine(void *arg)
{
    WorkerThread *worker = (WorkerThread *)arg;
    ThreadPoolHandle pool = worker->pool;
    set_worker_thread_name(worker->worker_idx);
    OST_LOG_INFO("Worker %d started", worker->worker_idx);

    pthread_mutex_lock(&worker->mutex);
    worker->state = WORKER_STATE_IDLE;
    pthread_cond_signal(&worker->cond_task);

    while (1) {
        while (!pool->is_destroying && worker->pending_req_head == NULL) {
            worker->state = WORKER_STATE_IDLE;
            pthread_cond_wait(&worker->cond_task, &worker->mutex);
        }
        if (pool->is_destroying) {
            worker->state = WORKER_STATE_EXIT;
            pthread_mutex_unlock(&worker->mutex);
            break;
        }

        uint32_t req_to_exec;
        TransportData user_data = {0};
        if (!worker_pending_req_pop(worker, &req_to_exec, &user_data)) {
            worker->state = WORKER_STATE_IDLE;
            continue;
        }

        ThreadPoolTask *task = worker_queue_pop_by_req(worker, req_to_exec);
        if (task) {
            worker->state = WORKER_STATE_BUSY;
            pthread_mutex_unlock(&worker->mutex);
            worker_process_task(worker, task, req_to_exec, &user_data);
            pthread_mutex_lock(&worker->mutex);
        } else {
            OST_LOG_WARN("Wakeup received without matching queued task (worker=%d, request_id=%u).",
                         worker->worker_idx,
                         req_to_exec);
            worker->state = WORKER_STATE_IDLE;
        }
    }
    OST_LOG_INFO("Worker %d exiting", worker->worker_idx);
    return NULL;
}

// 初始化同步对象
static void init_pool_sync(ThreadPoolHandle pool)
{
    pthread_mutex_init(&pool->task_id_mutex, NULL);
    pthread_mutex_init(&pool->global_mutex, NULL);
    pthread_mutex_init(&pool->start_mutex, NULL);
    pthread_mutex_init(&pool->req_hash_mutex, NULL);
    pthread_cond_init(&pool->cond_start, NULL);
}

// 初始化 worker 基础结构
static bool init_workers_basic(ThreadPoolHandle pool)
{
    pool->workers = calloc(pool->worker_count, sizeof(WorkerThread));
    if (!pool->workers) {
        OST_LOG_ERROR("Failed to allocate worker array (worker_count=%u)", pool->worker_count);
        return false;
    }

    for (uint32_t i = 0; i < pool->worker_count; i++) {
        WorkerThread *w = &pool->workers[i];
        pthread_mutex_init(&w->mutex, NULL);
        pthread_cond_init(&w->cond_task, NULL);
        w->state = WORKER_STATE_INIT;
        w->worker_idx = i;
        w->pool = pool;
        w->queue_head = w->queue_tail = NULL;
        w->queue_size = 0;
        w->pending_req_head = w->pending_req_tail = NULL;
        w->pending_req_count = 0;
    }

    return true;
}

// 创建单个 worker 线程
static bool create_worker(ThreadPoolHandle pool, int idx)
{
    WorkerThread *w = &pool->workers[idx];
    pthread_mutex_lock(&w->mutex);
    int ret = pthread_create(&w->tid, NULL, worker_routine, w);
    if (ret != 0) {
        OST_LOG_ERROR("Failed to create worker %d, pthread_create returned %d.", idx, ret);
        pthread_mutex_unlock(&w->mutex);
        return false;
    }

    while (w->state == WORKER_STATE_INIT) {
        pthread_cond_wait(&w->cond_task, &w->mutex);
    }
    pthread_mutex_unlock(&w->mutex);
    OST_LOG_INFO("Worker %d is ready.", idx);
    return true;
}

// 创建所有 worker 线程
static bool create_all_workers(ThreadPoolHandle pool)
{
    for (uint32_t i = 0; i < pool->worker_count; i++) {
        if (!create_worker(pool, i)) {
            return false;
        }
    }
    return true;
}

// 销毁同步对象
static void destroy_pool_sync(ThreadPoolHandle pool)
{
    pthread_mutex_destroy(&pool->task_id_mutex);
    pthread_mutex_destroy(&pool->global_mutex);
    pthread_mutex_destroy(&pool->start_mutex);
    pthread_mutex_destroy(&pool->req_hash_mutex);
    pthread_cond_destroy(&pool->cond_start);
}

// 设置销毁标志并唤醒所有线程
static void shutdown_threads(ThreadPoolHandle pool)
{
    pthread_mutex_lock(&pool->global_mutex);
    pool->is_destroying = true;
    pthread_mutex_unlock(&pool->global_mutex);

    OST_LOG_INFO("Shutting down worker threads (worker_count=%u).", pool->worker_count);

    for (uint32_t i = 0; i < pool->worker_count; i++) {
        WorkerThread *w = &pool->workers[i];
        pthread_mutex_lock(&w->mutex);
        pthread_cond_signal(&w->cond_task);
        pthread_mutex_unlock(&w->mutex);
    }
}

// 销毁单个 worker 的队列和锁
static void destroy_worker(WorkerThread *w)
{
    pthread_mutex_lock(&w->mutex);
    TaskNode *curr = w->queue_head;
    while (curr) {
        TaskNode *next = curr->next;
        free(curr->task->task_arg);
        free(curr->task);
        free(curr);
        curr = next;
    }
    worker_pending_req_clear(w);
    pthread_mutex_unlock(&w->mutex);
    pthread_mutex_destroy(&w->mutex);
    pthread_cond_destroy(&w->cond_task);
}

// 销毁所有 worker
static void destroy_all_workers(ThreadPoolHandle handle)
{
    if (!handle->workers) {
        return;
    }

    for (uint32_t i = 0; i < handle->worker_count; i++) {
        destroy_worker(&handle->workers[i]);
    }

    free(handle->workers);
    handle->workers = NULL;
}

// 销毁哈希表
static void destroy_hash_table(ThreadPoolHandle handle)
{
    for (int i = 0; i < REQ_HASH_SIZE; i++) {
        RequestContext *ctx = handle->req_hash[i];
        while (ctx) {
            RequestContext *next = ctx->next;
            free(ctx);
            ctx = next;
        }
    }
}

// 初始化线程池
ThreadPoolHandle thread_pool_init(uint32_t worker_thread_num, uint32_t pending_queue_cap)
{
    (void)pending_queue_cap;

    if (worker_thread_num == 0) {
        OST_LOG_ERROR("Invalid worker_thread_num: 0");
        return NULL;
    }

    ThreadPoolHandle pool = calloc(1, sizeof(struct _ThreadPool));
    if (!pool) {
        OST_LOG_ERROR("Failed: unable to allocate thread pool handle.");
        return NULL;
    }

    init_pool_sync(pool);
    pool->worker_count = worker_thread_num;
    if (!init_workers_basic(pool)) {
        destroy_pool_sync(pool);
        free(pool);
        return NULL;
    }

    memset(pool->req_hash, 0, sizeof(pool->req_hash));
    pool->next_task_id = 1;
    pool->is_running = false;
    pool->is_destroying = false;

    if (!create_all_workers(pool)) {
        thread_pool_destroy(pool);
        return NULL;
    }

    OST_LOG_INFO("Thread pool initialized with %u workers", pool->worker_count);
    return pool;
}

// 启动线程池
int thread_pool_start(ThreadPoolHandle handle)
{
    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL in thread_pool_start.");
        return -1;
    }
    if (handle->is_running) {
        OST_LOG_WARN("Thread pool start ignored because it is already running.");
        return -1;
    }
    handle->is_running = true;
    OST_LOG_INFO("Thread pool started");
    return 0;
}

int thread_pool_wake_up_worker_by_req_id(ThreadPoolHandle handle, uint32_t request_id, void *user_data)
{
    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL in thread_pool_wake_up_worker_by_req_id.");
        return -1;
    }

    int worker_idx = -1;

    pthread_mutex_lock(&handle->req_hash_mutex);
    RequestContext *ctx = find_req_context_locked(handle, request_id);
    if (ctx) {
        worker_idx = ctx->worker_idx;
    }
    pthread_mutex_unlock(&handle->req_hash_mutex);

    if (worker_idx < 0) {
        OST_LOG_WARN("No context for request_id %u", request_id);
        return -1;
    }
    WorkerThread *worker = &handle->workers[worker_idx];
    pthread_mutex_lock(&worker->mutex);
    if (!worker_pending_req_push(worker, request_id, user_data)) {
        pthread_mutex_unlock(&worker->mutex);
        OST_LOG_ERROR("Failed to enqueue pending request %u for worker %d", request_id, worker->worker_idx);
        return -1;
    }
    pthread_cond_signal(&worker->cond_task);
    pthread_mutex_unlock(&worker->mutex);
    return 0;
}

// 单任务提交
uint64_t thread_pool_submit_task(ThreadPoolHandle handle,
                                 uint32_t request_id,
                                 int (*task_func)(void *),
                                 void *task_arg,
                                 TaskCompleteCb complete_cb,
                                 void *user_data)
{
    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL in thread_pool_submit_task.");
        return 0;
    }
    if (!task_func) {
        OST_LOG_ERROR("Failed: task_func is NULL in thread_pool_submit_task (request_id=%u).", request_id);
        return 0;
    }
    if (!handle->is_running) {
        OST_LOG_WARN("Thread pool is not running when submitting task (request_id=%u).", request_id);
        return 0;
    }

    InternalTask *itask = malloc(sizeof(InternalTask));
    if (!itask) {
        OST_LOG_ERROR("Failed: unable to allocate InternalTask (request_id=%u).", request_id);
        return 0;
    }
    itask->user_func = task_func;
    itask->user_arg = task_arg;
    itask->prepare_cb = NULL;
    itask->complete_cb = complete_cb;
    itask->user_data = user_data;
    itask->request_id = request_id;
    itask->success = true;

    ThreadPoolTask *task = malloc(sizeof(ThreadPoolTask));
    if (!task) {
        OST_LOG_ERROR("Failed: unable to allocate ThreadPoolTask (request_id=%u).", request_id);
        free(itask);
        return 0;
    }
    task->task_id = generate_task_id(handle);
    task->request_id = request_id;
    task->prepare_cb = NULL;
    task->task_func = internal_task_wrapper;
    task->task_arg = itask;
    task->is_completed = false;
    itask->task_id = task->task_id;

    WorkerThread *worker = select_best_worker(handle);
    if (!worker) {
        OST_LOG_ERROR("No worker for task %lu", task->task_id);
        free(task);
        free(itask);
        return 0;
    }

    RequestContext *new_ctx = malloc(sizeof(RequestContext));
    if (!new_ctx) {
        OST_LOG_ERROR("Failed: unable to allocate RequestContext (request_id=%u).", request_id);
        free(task);
        free(itask);
        return 0;
    }
    new_ctx->request_id = request_id;
    new_ctx->worker_idx = worker->worker_idx;
    new_ctx->pending_count = 1;
    new_ctx->batch_cb = NULL;
    new_ctx->batch_user_data = NULL;
    new_ctx->next = NULL;

    pthread_mutex_lock(&worker->mutex);
    if (!worker_queue_push(worker, task)) {
        pthread_mutex_unlock(&worker->mutex);
        OST_LOG_ERROR("Failed: unable to enqueue task %lu to worker %d (request_id=%u).",
                      task->task_id,
                      worker->worker_idx,
                      request_id);
        free(new_ctx);
        free(task);
        free(itask);
        return 0;
    }
    pthread_mutex_unlock(&worker->mutex);

    pthread_mutex_lock(&handle->req_hash_mutex);
    RequestContext *ctx = find_req_context_locked(handle, request_id);
    if (ctx) {
        ctx->pending_count++;
        pthread_mutex_unlock(&handle->req_hash_mutex);
        free(new_ctx);
    } else {
        insert_req_context_locked(handle, new_ctx);
        pthread_mutex_unlock(&handle->req_hash_mutex);
    }
    OST_LOG_INFO("Task %lu (req=%u) submitted to worker %d", task->task_id, request_id, worker->worker_idx);
    return task->task_id;
}

// 创建单个批量任务节点
static bool create_batch_node(ThreadPoolHandle handle,
                              ThreadPoolTask *src,
                              uint32_t req_id,
                              TaskCompleteCb complete_cb,
                              void *user_data,
                              TaskNode **pnode,
                              uint64_t *task_id)
{
    InternalTask *itask = malloc(sizeof(InternalTask));
    if (!itask) {
        OST_LOG_ERROR("Failed: unable to allocate InternalTask for batch request_id=%u.", req_id);
        return false;
    }
    itask->user_func = src->task_func;
    itask->user_arg = src->task_arg;
    itask->prepare_cb = src->prepare_cb;
    itask->complete_cb = complete_cb;
    itask->user_data = user_data;
    itask->request_id = req_id;
    itask->success = true;

    ThreadPoolTask *task = malloc(sizeof(ThreadPoolTask));
    if (!task) {
        OST_LOG_ERROR("Failed: unable to allocate ThreadPoolTask for batch request_id=%u.", req_id);
        free(itask);
        return false;
    }
    task->task_id = generate_task_id(handle);
    task->request_id = req_id;
    task->prepare_cb = NULL;
    task->task_func = internal_task_wrapper;
    task->task_arg = itask;
    task->is_completed = false;
    itask->task_id = task->task_id;
    *task_id = task->task_id;

    TaskNode *node = malloc(sizeof(TaskNode));
    if (!node) {
        OST_LOG_ERROR("Failed: unable to allocate TaskNode for batch request_id=%u.", req_id);
        free(task);
        free(itask);
        return false;
    }
    node->task = task;
    node->next = NULL;
    *pnode = node;
    return true;
}

// 批量提交辅助：验证输入并选择 worker
static bool validate_and_select_worker(ThreadPoolHandle handle,
                                       ThreadPoolTask *tasks,
                                       uint32_t task_count,
                                       uint32_t *req_id,
                                       WorkerThread **worker,
                                       uint64_t **task_ids)
{
    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL in validate_and_select_worker.");
        return false;
    }
    if (!tasks || task_count == 0) {
        OST_LOG_ERROR("Failed: invalid task batch input (tasks=%p, task_count=%u).", (void *)tasks, task_count);
        return false;
    }
    if (!handle->is_running) {
        OST_LOG_WARN("Thread pool is not running when validating batch tasks.");
        return false;
    }
    *req_id = tasks[0].request_id;
    for (uint32_t i = 1; i < task_count; i++) {
        if (tasks[i].request_id != *req_id) {
            OST_LOG_ERROR("Failed: inconsistent request_id in batch submission "
                          "(expected=%u, actual=%u, index=%u).",
                          *req_id,
                          tasks[i].request_id,
                          i);
            return false;
        }
    }
    *task_ids = malloc(task_count * sizeof(uint64_t));
    if (!*task_ids) {
        OST_LOG_ERROR(
            "Failed: unable to allocate task_ids array for batch request_id=%u (task_count=%u).", *req_id, task_count);
        return false;
    }

    *worker = select_best_worker(handle);
    if (!*worker) {
        OST_LOG_ERROR("Failed: no available worker for batch request_id=%u.", *req_id);
        free(*task_ids);
        return false;
    }
    return true;
}

// 批量提交辅助：构造临时节点链表
static TaskNode *build_batch_nodes(ThreadPoolHandle handle,
                                   ThreadPoolTask *tasks,
                                   uint32_t task_count,
                                   uint32_t req_id,
                                   TaskCompleteCb complete_cb,
                                   void *user_data,
                                   uint64_t *task_ids,
                                   uint32_t *created)
{
    TaskNode *head = NULL;
    TaskNode *tail = NULL;
    *created = 0;
    for (uint32_t i = 0; i < task_count; i++) {
        TaskNode *node;
        if (!create_batch_node(handle, &tasks[i], req_id, complete_cb, user_data, &node, &task_ids[i])) {
            OST_LOG_ERROR("Failed: create_batch_node returned false "
                          "(request_id=%u, index=%u).",
                          req_id,
                          i);
            // 清理已创建的节点
            while (head) {
                TaskNode *tmp = head;
                head = head->next;
                free(tmp->task->task_arg);
                free(tmp->task);
                free(tmp);
            }
            return NULL;
        }
        if (tail) {
            tail->next = node;
            tail = node;
        } else {
            head = tail = node;
        }
        (*created)++;
    }
    return head;
}

// 批量提交辅助：挂载节点到 worker 队列
static void attach_nodes_to_worker(WorkerThread *worker, TaskNode *head, TaskNode *tail, uint32_t created)
{
    pthread_mutex_lock(&worker->mutex);
    if (worker->queue_tail) {
        worker->queue_tail->next = head;
    } else {
        worker->queue_head = head;
    }
    worker->queue_tail = tail;
    worker->queue_size += created;
    pthread_mutex_unlock(&worker->mutex);
}

// 批量提交辅助：更新上下文
static bool update_batch_context(ThreadPoolHandle handle,
                                 uint32_t req_id,
                                 int worker_idx,
                                 uint32_t task_count,
                                 TaskCompleteCb batch_complete_cb,
                                 void *batch_user_data)
{
    pthread_mutex_lock(&handle->req_hash_mutex);
    RequestContext *ctx = find_req_context_locked(handle, req_id);
    if (ctx) {
        ctx->pending_count += task_count;
        pthread_mutex_unlock(&handle->req_hash_mutex);
        return true;
    }

    ctx = malloc(sizeof(RequestContext));
    if (!ctx) {
        pthread_mutex_unlock(&handle->req_hash_mutex);
        OST_LOG_ERROR("Failed: unable to allocate RequestContext for batch request_id=%u.", req_id);
        return false;
    }
    ctx->request_id = req_id;
    ctx->worker_idx = worker_idx;
    ctx->pending_count = task_count;
    ctx->batch_cb = batch_complete_cb;
    ctx->batch_user_data = batch_user_data;
    ctx->next = NULL;
    insert_req_context_locked(handle, ctx);
    pthread_mutex_unlock(&handle->req_hash_mutex);
    return true;
}

// 批量提交主函数
uint64_t *thread_pool_submit_batch_tasks(ThreadPoolHandle handle,
                                         ThreadPoolTask *tasks,
                                         uint32_t task_count,
                                         TaskCompleteCb complete_cb,
                                         void *user_data,
                                         TaskCompleteCb batch_complete_cb,
                                         void *batch_user_data)
{
    uint32_t req_id;
    WorkerThread *target_worker;
    uint64_t *task_ids;

    if (!validate_and_select_worker(handle, tasks, task_count, &req_id, &target_worker, &task_ids)) {
        OST_LOG_ERROR("Failed: validate_and_select_worker returned false for batch submission.");
        return NULL;
    }

    uint32_t created = 0;
    TaskNode *head = build_batch_nodes(handle, tasks, task_count, req_id, complete_cb, user_data, task_ids, &created);
    if (!head) {
        OST_LOG_ERROR("Failed: build_batch_nodes returned NULL (request_id=%u, task_count=%u).", req_id, task_count);
        free(task_ids);
        return NULL;
    }

    TaskNode *tail = head;
    while (tail->next)
        tail = tail->next;

    attach_nodes_to_worker(target_worker, head, tail, created);

    if (!update_batch_context(
            handle, req_id, target_worker->worker_idx, task_count, batch_complete_cb, batch_user_data)) {
        OST_LOG_ERROR("Failed to update context for req %u", req_id);
    }
    OST_LOG_INFO("Batch of %u tasks (req=%u) submitted to worker %d", task_count, req_id, target_worker->worker_idx);
    return task_ids;
}

// 从 worker 队列中取消指定 request_id 的任务（返回移除数量）
static uint32_t cancel_in_worker_queue(WorkerThread *worker, uint32_t req_id)
{
    pthread_mutex_lock(&worker->mutex);
    TaskNode *prev = NULL;
    TaskNode *curr = worker->queue_head;
    uint32_t removed = 0;
    while (curr) {
        if (curr->task->request_id == req_id) {
            TaskNode *to_free = curr;
            if (prev) {
                prev->next = curr->next;
            } else {
                worker->queue_head = curr->next;
            }
            curr = curr->next;
            if (to_free == worker->queue_tail) {
                worker->queue_tail = prev;
            }
            free(to_free->task->task_arg);
            free(to_free->task);
            free(to_free);
            removed++;
        } else {
            prev = curr;
            curr = curr->next;
        }
    }
    worker->queue_size -= removed;
    uint32_t removed_pending = worker_pending_req_remove_by_req(worker, req_id);
    pthread_mutex_unlock(&worker->mutex);
    if (removed_pending > 0) {
        OST_LOG_INFO(
            "Removed %u pending wakeups for req %u from worker %d.", removed_pending, req_id, worker->worker_idx);
    }
    return removed;
}

// 取消后更新上下文
static void update_context_after_cancel(ThreadPoolHandle handle, uint32_t request_id, uint32_t removed)
{
    pthread_mutex_lock(&handle->req_hash_mutex);
    RequestContext *ctx = find_req_context_locked(handle, request_id);
    if (ctx) {
        if (removed >= (uint32_t)ctx->pending_count) {
            (void)remove_req_context_locked(handle, request_id);
        } else {
            ctx->pending_count -= (int)removed;
        }
    }
    pthread_mutex_unlock(&handle->req_hash_mutex);
}

// 根据 request_id 销毁所有未执行的任务
int thread_pool_cancel_tasks_by_req(ThreadPoolHandle handle, uint32_t request_id)
{
    if (!handle) {
        OST_LOG_ERROR("Failed: handle is NULL in thread_pool_cancel_tasks_by_req.");
        return -1;
    }
    if (!handle->is_running) {
        OST_LOG_WARN("Cancel request ignored because thread pool is not running (request_id=%u).", request_id);
        return -1;
    }

    int worker_idx = -1;

    pthread_mutex_lock(&handle->req_hash_mutex);
    RequestContext *ctx = find_req_context_locked(handle, request_id);
    if (ctx) {
        worker_idx = ctx->worker_idx;
    }
    pthread_mutex_unlock(&handle->req_hash_mutex);

    if (worker_idx < 0) {
        OST_LOG_INFO("No request context found when canceling request_id=%u.", request_id);
        return 0;
    }

    uint32_t removed = cancel_in_worker_queue(&handle->workers[worker_idx], request_id);
    if (removed > 0) {
        update_context_after_cancel(handle, request_id, removed);
        OST_LOG_INFO("Canceled %u queued tasks for request_id=%u.", removed, request_id);
    } else {
        OST_LOG_INFO("No queued tasks needed cancellation for request_id=%u.", request_id);
    }
    return (int)removed;
}

// 销毁线程池
void thread_pool_destroy(ThreadPoolHandle handle)
{
    if (!handle)
        return;
    OST_LOG_INFO("Destroying thread pool...");

    shutdown_threads(handle);

    for (uint32_t i = 0; i < handle->worker_count; i++) {
        if (handle->workers[i].tid)
            pthread_join(handle->workers[i].tid, NULL);
    }

    destroy_all_workers(handle);
    destroy_hash_table(handle);
    destroy_pool_sync(handle);
    free(handle);
    OST_LOG_INFO("Thread pool destroyed");
}

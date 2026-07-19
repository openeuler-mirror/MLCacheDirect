/*
 * test_thread_pool.c - 线程池单元测试
 *
 * 编译命令：
 *   gcc -g -o test_thread_pool src/os_transport_thread_pool.c src/os_transport_log.c test/test_thread_pool.c -lpthread -Iinclude
 */

#include "os_transport_thread_pool.h"
#include "os_transport_thread_pool_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

/* ---------- 测试全局状态 ---------- */
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int completed_count;       // 单个任务完成计数
    int batch_completed_count; // 批次完成计数
    int *exec_order;           // 记录每个任务执行的序号（按完成顺序）
    int exec_index;
    int total_tasks; // 预期总任务数
} TestState;

static TestState g_state = {0};
static bool g_state_sync_initialized = false;

static void test_state_init(int total)
{
    if (!g_state_sync_initialized) {
        pthread_mutex_init(&g_state.lock, NULL);
        pthread_cond_init(&g_state.cond, NULL);
        g_state_sync_initialized = true;
    }
    g_state.completed_count = 0;
    g_state.batch_completed_count = 0;
    if (g_state.exec_order)
        free(g_state.exec_order);
    g_state.exec_order = calloc(total, sizeof(int));
    g_state.exec_index = 0;
    g_state.total_tasks = total;
}

static void wake_request(ThreadPoolHandle pool, uint32_t req_id)
{
    assert(thread_pool_wake_up_worker_by_req_id(pool, req_id, NULL) == 0);
}

static RequestContext *find_test_req_context(ThreadPoolHandle pool, uint32_t req_id)
{
    for (int i = 0; i < REQ_HASH_SIZE; i++) {
        RequestContext *ctx = pool->req_hash[i];
        while (ctx) {
            if (ctx->request_id == req_id) {
                return ctx;
            }
            ctx = ctx->next;
        }
    }
    return NULL;
}

static void append_test_pending_req(WorkerThread *worker, uint32_t req_id, const TransportData *user_data)
{
    PendingReqNode *node = calloc(1, sizeof(PendingReqNode));
    assert(node != NULL);
    node->request_id = req_id;
    if (user_data) {
        node->user_data = *user_data;
    }

    pthread_mutex_lock(&worker->mutex);
    if (worker->pending_req_tail) {
        worker->pending_req_tail->next = node;
    } else {
        worker->pending_req_head = node;
    }
    worker->pending_req_tail = node;
    worker->pending_req_count++;
    pthread_mutex_unlock(&worker->mutex);
}

static void test_state_wait_completion(void)
{
    pthread_mutex_lock(&g_state.lock);
    while (g_state.completed_count < g_state.total_tasks) {
        pthread_cond_wait(&g_state.cond, &g_state.lock);
    }
    pthread_mutex_unlock(&g_state.lock);
}

static void test_state_wait_batch(int expected_batches)
{
    pthread_mutex_lock(&g_state.lock);
    while (g_state.batch_completed_count < expected_batches) {
        pthread_cond_wait(&g_state.cond, &g_state.lock);
    }
    pthread_mutex_unlock(&g_state.lock);
}

/* ---------- 任务函数 ---------- */
static int test_task(void *arg)
{
    int seq = *(int *)arg;
    printf("Executing task seq %d in thread %lu\n", seq, (unsigned long)pthread_self());

    pthread_mutex_lock(&g_state.lock);
    g_state.exec_order[g_state.exec_index++] = seq;
    pthread_mutex_unlock(&g_state.lock);

    free(arg);
    return 0;
}

typedef struct {
    int seq;
    uint64_t observed_user_ctx;
} MatchTaskArg;

static void prepare_match_task(void *task_arg, void *user_data)
{
    MatchTaskArg *arg = (MatchTaskArg *)task_arg;
    TransportData *data = (TransportData *)user_data;
    assert(arg != NULL);
    assert(data != NULL);
    arg->observed_user_ctx = data->user_ctx;
}

static int matched_task(void *arg)
{
    MatchTaskArg *match_arg = (MatchTaskArg *)arg;
    assert(match_arg != NULL);
    assert(match_arg->observed_user_ctx != 0);

    pthread_mutex_lock(&g_state.lock);
    g_state.exec_order[g_state.exec_index++] = match_arg->seq;
    pthread_mutex_unlock(&g_state.lock);

    free(match_arg);
    return 0;
}

/* ---------- 回调函数 ---------- */
static void test_complete_cb(uint64_t task_id, bool success, void *user_data)
{
    (void)user_data;
    (void)success;
    pthread_mutex_lock(&g_state.lock);
    g_state.completed_count++;
    pthread_cond_signal(&g_state.cond);
    pthread_mutex_unlock(&g_state.lock);
    printf("Task %lu completed\n", task_id);
}

static void batch_complete_cb(uint64_t task_id, bool success, void *user_data)
{
    (void)task_id;
    (void)success;
    uint32_t req_id = (uint32_t)(uintptr_t)user_data;
    printf("Batch complete for req %u\n", req_id);
    pthread_mutex_lock(&g_state.lock);
    g_state.batch_completed_count++;
    pthread_cond_signal(&g_state.cond);
    pthread_mutex_unlock(&g_state.lock);
}

/* ---------- 测试用例 ---------- */

// 测试1：单个任务
static void test_single_tasks(ThreadPoolHandle pool)
{
    printf("\n=== Test 1: Single tasks ===\n");
    test_state_init(2);
    uint32_t req1 = 1001, req2 = 1002;
    int *arg1 = malloc(sizeof(int));
    *arg1 = 1;
    int *arg2 = malloc(sizeof(int));
    *arg2 = 2;

    uint64_t id1 = thread_pool_submit_task(pool, req1, test_task, arg1, test_complete_cb, NULL);
    uint64_t id2 = thread_pool_submit_task(pool, req2, test_task, arg2, test_complete_cb, NULL);
    assert(id1 != 0 && id2 != 0);
    printf("Submitted tasks: %lu, %lu\n", id1, id2);

    wake_request(pool, req1);
    wake_request(pool, req2);

    test_state_wait_completion();
    assert((g_state.exec_order[0] == 1 && g_state.exec_order[1] == 2) ||
           (g_state.exec_order[0] == 2 && g_state.exec_order[1] == 1));
    printf("Test 1 passed.\n");
}

// 测试2：批量任务
static void test_batch_tasks(ThreadPoolHandle pool)
{
    printf("\n=== Test 2: Batch tasks ===\n");
    const int BATCH = 5;
    uint32_t batch_req = 2001;
    ThreadPoolTask tasks[BATCH];
    int *args[BATCH];

    test_state_init(BATCH);
    memset(tasks, 0, sizeof(tasks));
    for (int i = 0; i < BATCH; i++) {
        args[i] = malloc(sizeof(int));
        *args[i] = i + 10;
        tasks[i].request_id = batch_req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
        tasks[i].free_task_self = false;
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(
        pool, tasks, BATCH, test_complete_cb, NULL, batch_complete_cb, (void *)(uintptr_t)batch_req);
    assert(ids != NULL);
    free(ids);

    for (int i = 0; i < BATCH; i++) {
        wake_request(pool, batch_req);
        usleep(20000); // 让 worker 处理
    }

    test_state_wait_completion();
    test_state_wait_batch(1);
    for (int i = 0; i < BATCH; i++) {
        assert(g_state.exec_order[i] == 10 + i);
    }
    printf("Test 2 passed.\n");
}

// 测试3：交错通知
static void test_interleaved(ThreadPoolHandle pool)
{
    printf("\n=== Test 3: Interleaved ===\n");
    const int N = 3;
    uint32_t req_a = 3001, req_b = 3002;
    ThreadPoolTask ta[N], tb[N];
    int *sa[N], *sb[N];

    test_state_init(N * 2);
    memset(ta, 0, sizeof(ta));
    memset(tb, 0, sizeof(tb));
    for (int i = 0; i < N; i++) {
        sa[i] = malloc(sizeof(int));
        *sa[i] = 100 + i;
        ta[i].request_id = req_a;
        ta[i].task_func = test_task;
        ta[i].task_arg = sa[i];
        sb[i] = malloc(sizeof(int));
        *sb[i] = 200 + i;
        tb[i].request_id = req_b;
        tb[i].task_func = test_task;
        tb[i].task_arg = sb[i];
    }

    uint64_t *ida = thread_pool_submit_batch_tasks(
        pool, ta, N, test_complete_cb, NULL, batch_complete_cb, (void *)(uintptr_t)req_a);
    uint64_t *idb = thread_pool_submit_batch_tasks(
        pool, tb, N, test_complete_cb, NULL, batch_complete_cb, (void *)(uintptr_t)req_b);
    assert(ida && idb);
    free(ida);
    free(idb);

    // 交错发送
    wake_request(pool, req_a);
    usleep(20000);
    wake_request(pool, req_b);
    usleep(20000);
    wake_request(pool, req_a);
    usleep(20000);
    wake_request(pool, req_b);
    usleep(20000);
    wake_request(pool, req_a);
    usleep(20000);
    wake_request(pool, req_b);
    usleep(20000);

    test_state_wait_completion();
    test_state_wait_batch(2);
    int ea[N];
    int eb[N];
    memset(ea, 0, sizeof(ea));
    memset(eb, 0, sizeof(eb));
    int ca = 0, cb = 0;
    for (int i = 0; i < g_state.exec_index; i++) {
        int v = g_state.exec_order[i];
        if (v >= 100 && v < 200)
            ea[ca++] = v;
        else if (v >= 200 && v < 300)
            eb[cb++] = v;
    }
    assert(ca == N && cb == N);
    for (int i = 0; i < N; i++) {
        assert(ea[i] == 100 + i);
        assert(eb[i] == 200 + i);
    }
    printf("Test 3 passed.\n");
}

// 测试4：大量任务（队列扩容）
static void test_many(ThreadPoolHandle pool)
{
    printf("\n=== Test 4: Many tasks ===\n");
    const int LARGE = 100;
    uint32_t req = 4001;
    ThreadPoolTask tasks[LARGE];
    int *args[LARGE];

    test_state_init(LARGE);
    memset(tasks, 0, sizeof(tasks));
    for (int i = 0; i < LARGE; i++) {
        args[i] = malloc(sizeof(int));
        *args[i] = i;
        tasks[i].request_id = req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(
        pool, tasks, LARGE, test_complete_cb, NULL, batch_complete_cb, (void *)(uintptr_t)req);
    assert(ids != NULL);
    free(ids);

    for (int i = 0; i < LARGE; i++) {
        wake_request(pool, req);
    }

    test_state_wait_completion();
    test_state_wait_batch(1);
    printf("Test 4 passed (%d tasks).\n", LARGE);
}

// 测试5：取消任务
static void test_cancel(ThreadPoolHandle pool)
{
    printf("\n=== Test 5: Cancel tasks ===\n");
    const int TOTAL = 10;
    uint32_t req = 5001;
    ThreadPoolTask tasks[TOTAL];
    int *args[TOTAL];

    test_state_init(TOTAL);
    memset(tasks, 0, sizeof(tasks));
    for (int i = 0; i < TOTAL; i++) {
        args[i] = malloc(sizeof(int));
        *args[i] = i;
        tasks[i].request_id = req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(
        pool, tasks, TOTAL, test_complete_cb, NULL, batch_complete_cb, (void *)(uintptr_t)req);
    assert(ids != NULL);
    free(ids);

    // 取消所有任务
    int canceled = thread_pool_cancel_tasks_by_req(pool, req);
    assert(canceled == TOTAL);

    // 取消后不存在request上下文，唤醒应失败且不执行任务
    assert(thread_pool_wake_up_worker_by_req_id(pool, req, NULL) == -1);
    usleep(100000);

    pthread_mutex_lock(&g_state.lock);
    assert(g_state.completed_count == 0);
    pthread_mutex_unlock(&g_state.lock);
    printf("Test 5 passed.\n");
}

// 测试6：非法参数和未启动状态
static void test_invalid_inputs(void)
{
    printf("\n=== Test 6: Invalid inputs ===\n");

    assert(thread_pool_init(0, 0) == NULL);
    assert(thread_pool_start(NULL) == -1);
    assert(thread_pool_wake_up_worker_by_req_id(NULL, 1, NULL) == -1);
    assert(thread_pool_submit_task(NULL, 1, test_task, NULL, test_complete_cb, NULL) == 0);
    assert(thread_pool_submit_batch_tasks(NULL, NULL, 0, NULL, NULL, NULL, NULL) == NULL);
    assert(thread_pool_cancel_tasks_by_req(NULL, 1) == -1);
    thread_pool_destroy(NULL);

    ThreadPoolHandle stopped = thread_pool_init(1, 0);
    assert(stopped != NULL);
    int *arg = malloc(sizeof(int));
    assert(arg != NULL);
    *arg = 600;
    assert(thread_pool_submit_task(stopped, 6001, test_task, arg, test_complete_cb, NULL) == 0);
    free(arg);
    assert(thread_pool_cancel_tasks_by_req(stopped, 6001) == -1);
    thread_pool_destroy(stopped);

    ThreadPoolHandle started = thread_pool_init(1, 0);
    assert(started != NULL);
    assert(thread_pool_start(started) == 0);
    assert(thread_pool_start(started) == -1);
    assert(thread_pool_submit_task(started, 6002, NULL, NULL, test_complete_cb, NULL) == 0);

    ThreadPoolTask bad_tasks[2] = {0};
    bad_tasks[0].request_id = 6003;
    bad_tasks[0].task_func = test_task;
    bad_tasks[1].request_id = 6004;
    bad_tasks[1].task_func = test_task;
    assert(thread_pool_submit_batch_tasks(started, NULL, 1, test_complete_cb, NULL, NULL, NULL) == NULL);
    assert(thread_pool_submit_batch_tasks(started, bad_tasks, 0, test_complete_cb, NULL, NULL, NULL) == NULL);
    assert(thread_pool_submit_batch_tasks(started, bad_tasks, 2, test_complete_cb, NULL, NULL, NULL) == NULL);
    assert(thread_pool_wake_up_worker_by_req_id(started, 6005, NULL) == -1);
    assert(thread_pool_cancel_tasks_by_req(started, 6005) == 0);
    thread_pool_destroy(started);

    printf("Test 6 passed.\n");
}

// 测试7：completion user_data 精确匹配和乱序唤醒
static void test_match_user_ctx(ThreadPoolHandle pool)
{
    printf("\n=== Test 7: Match user ctx ===\n");
    uint32_t req = 7001;
    ThreadPoolTask tasks[3];
    MatchTaskArg *args[3];
    TransportData data[3];

    test_state_init(3);
    memset(tasks, 0, sizeof(tasks));
    memset(data, 0, sizeof(data));

    for (int i = 0; i < 3; i++) {
        args[i] = calloc(1, sizeof(MatchTaskArg));
        assert(args[i] != NULL);
        args[i]->seq = i + 1;
        data[i].bs.request_id = req;
        data[i].bs.chunk_id = (uint64_t)(i + 1);
        data[i].bs.chunk_type = (i == 2) ? 2U : 1U;
        data[i].bs.chunk_size = 128U + (uint64_t)i;

        tasks[i].request_id = req;
        tasks[i].prepare_cb = prepare_match_task;
        tasks[i].task_func = matched_task;
        tasks[i].task_arg = args[i];
        tasks[i].has_match_user_ctx = true;
        tasks[i].match_user_ctx = data[i].user_ctx;
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(
        pool, tasks, 3, test_complete_cb, NULL, batch_complete_cb, (void *)(uintptr_t)req);
    assert(ids != NULL);
    free(ids);

    TransportData wrong = data[0];
    wrong.bs.chunk_id = 15;
    assert(thread_pool_wake_up_worker_by_req_id(pool, req, &wrong) == 0);
    usleep(50000);

    assert(thread_pool_wake_up_worker_by_req_id(pool, req, &data[2]) == 0);
    assert(thread_pool_wake_up_worker_by_req_id(pool, req, &data[0]) == 0);
    assert(thread_pool_wake_up_worker_by_req_id(pool, req, &data[1]) == 0);

    test_state_wait_completion();
    test_state_wait_batch(1);
    assert(g_state.exec_order[0] == 3);
    assert(g_state.exec_order[1] == 1);
    assert(g_state.exec_order[2] == 2);
    printf("Test 7 passed.\n");
}

// 测试8：取消时清理pending wakeup和销毁残留队列
static void test_cancel_pending_and_destroy_queued(void)
{
    printf("\n=== Test 8: Cancel pending and destroy queued ===\n");

    ThreadPoolHandle cancel_pool = thread_pool_init(1, 0);
    assert(cancel_pool != NULL);
    assert(thread_pool_start(cancel_pool) == 0);

    uint32_t req = 8001;
    ThreadPoolTask tasks[2] = {0};
    int *args[2];
    for (int i = 0; i < 2; i++) {
        args[i] = malloc(sizeof(int));
        assert(args[i] != NULL);
        *args[i] = i;
        tasks[i].request_id = req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
    }

    uint64_t *ids =
        thread_pool_submit_batch_tasks(cancel_pool, tasks, 2, test_complete_cb, NULL, batch_complete_cb, NULL);
    assert(ids != NULL);
    free(ids);

    pthread_mutex_lock(&cancel_pool->req_hash_mutex);
    RequestContext *ctx = find_test_req_context(cancel_pool, req);
    assert(ctx != NULL);
    int worker_idx = ctx->worker_idx;
    pthread_mutex_unlock(&cancel_pool->req_hash_mutex);

    TransportData pending_data = {0};
    pending_data.bs.request_id = req;
    pending_data.bs.chunk_id = 9;
    append_test_pending_req(&cancel_pool->workers[worker_idx], req, &pending_data);

    int canceled = thread_pool_cancel_tasks_by_req(cancel_pool, req);
    assert(canceled == 2);
    assert(cancel_pool->workers[worker_idx].pending_req_count == 0);
    thread_pool_destroy(cancel_pool);

    ThreadPoolHandle destroy_pool = thread_pool_init(1, 0);
    assert(destroy_pool != NULL);
    assert(thread_pool_start(destroy_pool) == 0);
    int *queued_arg = malloc(sizeof(int));
    assert(queued_arg != NULL);
    *queued_arg = 42;
    uint64_t queued_id = thread_pool_submit_task(destroy_pool, 8002, test_task, queued_arg, test_complete_cb, NULL);
    assert(queued_id != 0);
    thread_pool_destroy(destroy_pool);

    printf("Test 8 passed.\n");
}

// 测试9：销毁
static void test_destroy(ThreadPoolHandle pool)
{
    printf("\n=== Test 9: Destroy ===\n");
    thread_pool_destroy(pool);
    printf("Test 9 passed.\n");
}

/* ---------- 主函数 ---------- */
int main(void)
{
    printf("Starting thread pool tests...\n");

    // 初始化线程池（2个worker）
    ThreadPoolHandle pool = thread_pool_init(2, 0);
    assert(pool != NULL);

    int ret = thread_pool_start(pool);
    assert(ret == 0);
    printf("Thread pool started.\n");

    test_single_tasks(pool);
    test_batch_tasks(pool);
    test_interleaved(pool);
    test_many(pool);
    test_cancel(pool);
    test_invalid_inputs();
    test_match_user_ctx(pool);
    test_cancel_pending_and_destroy_queued();
    test_destroy(pool);

    free(g_state.exec_order);
    if (g_state_sync_initialized) {
        pthread_mutex_destroy(&g_state.lock);
        pthread_cond_destroy(&g_state.cond);
    }

    printf("\nAll tests passed!\n");
    return 0;
}

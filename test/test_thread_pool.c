/*
 * test_thread_pool.c - 线程池单元测试（TEST_MODE 模拟 URMA 事件）
 *
 * 编译命令：
 *   gcc -DTEST_MODE -g -o test_thread_pool os_transport_thread_pool.c test_thread_pool.c -lpthread -I.
 */

#include "os_transport_thread_pool.h"
#include "os_transport_thread_pool_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include <pthread.h>

/* ---------- 声明测试模式下的模拟队列函数 ---------- */
#ifdef TEST_MODE
void mock_event_queue_init(uint32_t cap);
void mock_event_queue_push(uint64_t req_id);
void mock_event_queue_destroy(void);
#endif

/* ---------- 测试全局状态 ---------- */
typedef struct {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    int completed_count;          // 单个任务完成计数
    int batch_completed_count;     // 批次完成计数
    int *exec_order;               // 记录每个任务执行的序号（按完成顺序）
    int exec_index;
    int total_tasks;               // 预期总任务数
} TestState;

static TestState g_state = {0};

static void test_state_init(int total) {
    pthread_mutex_init(&g_state.lock, NULL);
    pthread_cond_init(&g_state.cond, NULL);
    g_state.completed_count = 0;
    g_state.batch_completed_count = 0;
    if (g_state.exec_order) free(g_state.exec_order);
    g_state.exec_order = calloc(total, sizeof(int));
    g_state.exec_index = 0;
    g_state.total_tasks = total;
}

static void test_state_wait_completion(void) {
    pthread_mutex_lock(&g_state.lock);
    while (g_state.completed_count < g_state.total_tasks) {
        pthread_cond_wait(&g_state.cond, &g_state.lock);
    }
    pthread_mutex_unlock(&g_state.lock);
}

static void test_state_wait_batch(int expected_batches) {
    pthread_mutex_lock(&g_state.lock);
    while (g_state.batch_completed_count < expected_batches) {
        pthread_cond_wait(&g_state.cond, &g_state.lock);
    }
    pthread_mutex_unlock(&g_state.lock);
}

/* ---------- 任务函数 ---------- */
static int test_task(void *arg) {
    int seq = *(int *)arg;
    printf("Executing task seq %d in thread %lu\n", seq, (unsigned long)pthread_self());

    pthread_mutex_lock(&g_state.lock);
    g_state.exec_order[g_state.exec_index++] = seq;
    pthread_mutex_unlock(&g_state.lock);

    free(arg);
    return 0;
}

/* ---------- 回调函数 ---------- */
static void test_complete_cb(uint64_t task_id, bool success, void *user_data) {
    (void)user_data;
    pthread_mutex_lock(&g_state.lock);
    g_state.completed_count++;
    pthread_cond_signal(&g_state.cond);
    pthread_mutex_unlock(&g_state.lock);
    printf("Task %lu completed\n", task_id);
}

static void batch_complete_cb(uint64_t task_id, bool success, void *user_data) {
    uint32_t req_id = (uint32_t)(uintptr_t)user_data;
    printf("Batch complete for req %u\n", req_id);
    pthread_mutex_lock(&g_state.lock);
    g_state.batch_completed_count++;
    pthread_cond_signal(&g_state.cond);
    pthread_mutex_unlock(&g_state.lock);
}

/* ---------- 测试用例 ---------- */

// 测试1：单个任务
static void test_single_tasks(ThreadPoolHandle pool) {
    printf("\n=== Test 1: Single tasks ===\n");
    test_state_init(2);
    uint32_t req1 = 1001, req2 = 1002;
    int *arg1 = malloc(sizeof(int)); *arg1 = 1;
    int *arg2 = malloc(sizeof(int)); *arg2 = 2;

    uint64_t id1 = thread_pool_submit_task(pool, req1, test_task, arg1, test_complete_cb, NULL);
    uint64_t id2 = thread_pool_submit_task(pool, req2, test_task, arg2, test_complete_cb, NULL);
    assert(id1 != 0 && id2 != 0);
    printf("Submitted tasks: %lu, %lu\n", id1, id2);

    mock_event_queue_push(req1);
    mock_event_queue_push(req2);

    test_state_wait_completion();
    assert(g_state.exec_order[0] == 1 && g_state.exec_order[1] == 2);
    printf("Test 1 passed.\n");
}

// 测试2：批量任务
static void test_batch_tasks(ThreadPoolHandle pool) {
    printf("\n=== Test 2: Batch tasks ===\n");
    const int BATCH = 5;
    uint32_t batch_req = 2001;
    ThreadPoolTask tasks[BATCH];
    int *args[BATCH];

    test_state_init(BATCH);
    for (int i = 0; i < BATCH; i++) {
        args[i] = malloc(sizeof(int));
        *args[i] = i + 10;
        tasks[i].request_id = batch_req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
        tasks[i].free_task_self = false;
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(pool, tasks, BATCH,
                                                   test_complete_cb, NULL,
                                                   batch_complete_cb, (void*)(uintptr_t)batch_req);
    assert(ids != NULL);
    free(ids);

    for (int i = 0; i < BATCH; i++) {
        mock_event_queue_push(batch_req);
        usleep(20000); // 让 asyncPoll 处理
    }

    test_state_wait_completion();
    test_state_wait_batch(1);
    for (int i = 0; i < BATCH; i++) {
        assert(g_state.exec_order[i] == 10 + i);
    }
    printf("Test 2 passed.\n");
}

// 测试3：交错通知
static void test_interleaved(ThreadPoolHandle pool) {
    printf("\n=== Test 3: Interleaved ===\n");
    const int N = 3;
    uint32_t req_a = 3001, req_b = 3002;
    ThreadPoolTask ta[N], tb[N];
    int *sa[N], *sb[N];

    test_state_init(N * 2);
    for (int i = 0; i < N; i++) {
        sa[i] = malloc(sizeof(int)); *sa[i] = 100 + i;
        ta[i].request_id = req_a; ta[i].task_func = test_task; ta[i].task_arg = sa[i];
        sb[i] = malloc(sizeof(int)); *sb[i] = 200 + i;
        tb[i].request_id = req_b; tb[i].task_func = test_task; tb[i].task_arg = sb[i];
    }

    uint64_t *ida = thread_pool_submit_batch_tasks(pool, ta, N, test_complete_cb, NULL,
                                                   batch_complete_cb, (void*)(uintptr_t)req_a);
    uint64_t *idb = thread_pool_submit_batch_tasks(pool, tb, N, test_complete_cb, NULL,
                                                   batch_complete_cb, (void*)(uintptr_t)req_b);
    assert(ida && idb);
    free(ida); free(idb);

    // 交错发送
    mock_event_queue_push(req_a); usleep(20000);
    mock_event_queue_push(req_b); usleep(20000);
    mock_event_queue_push(req_a); usleep(20000);
    mock_event_queue_push(req_b); usleep(20000);
    mock_event_queue_push(req_a); usleep(20000);
    mock_event_queue_push(req_b); usleep(20000);

    test_state_wait_completion();
    test_state_wait_batch(2);
    int ea[N];
    int eb[N];
    memset(ea, 0, sizeof(ea));
    memset(eb, 0, sizeof(eb));
    int ca = 0, cb = 0;
    for (int i = 0; i < g_state.exec_index; i++) {
        int v = g_state.exec_order[i];
        if (v >= 100 && v < 200) ea[ca++] = v;
        else if (v >= 200 && v < 300) eb[cb++] = v;
    }
    assert(ca == N && cb == N);
    for (int i = 0; i < N; i++) {
        assert(ea[i] == 100 + i);
        assert(eb[i] == 200 + i);
    }
    printf("Test 3 passed.\n");
}

// 测试4：大量任务（队列扩容）
static void test_many(ThreadPoolHandle pool) {
    printf("\n=== Test 4: Many tasks ===\n");
    const int LARGE = 100;
    uint32_t req = 4001;
    ThreadPoolTask tasks[LARGE];
    int *args[LARGE];

    test_state_init(LARGE);
    for (int i = 0; i < LARGE; i++) {
        args[i] = malloc(sizeof(int));
        *args[i] = i;
        tasks[i].request_id = req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(pool, tasks, LARGE, test_complete_cb, NULL,
                                                   batch_complete_cb, (void*)(uintptr_t)req);
    assert(ids != NULL);
    free(ids);

    for (int i = 0; i < LARGE; i++) {
        mock_event_queue_push(req);
    }

    test_state_wait_completion();
    test_state_wait_batch(1);
    printf("Test 4 passed (%d tasks).\n", LARGE);
}

// 测试5：取消任务
static void test_cancel(ThreadPoolHandle pool) {
    printf("\n=== Test 5: Cancel tasks ===\n");
    const int TOTAL = 10;
    uint32_t req = 5001;
    ThreadPoolTask tasks[TOTAL];
    int *args[TOTAL];

    for (int i = 0; i < TOTAL; i++) {
        args[i] = malloc(sizeof(int));
        *args[i] = i;
        tasks[i].request_id = req;
        tasks[i].task_func = test_task;
        tasks[i].task_arg = args[i];
    }

    uint64_t *ids = thread_pool_submit_batch_tasks(pool, tasks, TOTAL,
                                                   test_complete_cb, NULL,
                                                   batch_complete_cb, (void*)(uintptr_t)req);
    assert(ids != NULL);
    free(ids);

    // 取消所有任务
    int canceled = thread_pool_cancel_tasks_by_req(pool, req);
    assert(canceled == TOTAL);

    // 发送通知，不应有任务执行
    mock_event_queue_push(req);
    usleep(100000);

    pthread_mutex_lock(&g_state.lock);
    assert(g_state.completed_count == 0);
    pthread_mutex_unlock(&g_state.lock);
    printf("Test 5 passed.\n");
}

// 测试6：销毁
static void test_destroy(ThreadPoolHandle pool) {
    printf("\n=== Test 6: Destroy ===\n");
    thread_pool_destroy(pool);
    printf("Test 6 passed.\n");
}

/* ---------- 主函数 ---------- */
int main(void) {
    printf("Starting thread pool tests (TEST_MODE enabled)...\n");

    // 初始化模拟事件队列
    mock_event_queue_init(64);

    // 初始化线程池（链表不需要容量）
    ThreadPoolHandle pool = thread_pool_init(2, 0);
    assert(pool != NULL);

    // 设置 URMA 信息（测试模式下不会真正使用，但需避免空指针）
    static urma_jfce_t dummy_jfce;
    static urma_jfc_t dummy_jfc;
    pool->urmaInfo.jfce = &dummy_jfce;
    pool->urmaInfo.jfc = &dummy_jfc;
    pool->urmaInfo.urma_event_mode = false;

    int ret = thread_pool_start(pool);
    assert(ret == 0);
    printf("Thread pool started.\n");

    test_single_tasks(pool);
    test_batch_tasks(pool);
    test_interleaved(pool);
    test_many(pool);
    test_cancel(pool);
    test_destroy(pool);

    mock_event_queue_destroy();
    free(g_state.exec_order);
    pthread_mutex_destroy(&g_state.lock);
    pthread_cond_destroy(&g_state.cond);

    printf("\nAll tests passed!\n");
    return 0;
}
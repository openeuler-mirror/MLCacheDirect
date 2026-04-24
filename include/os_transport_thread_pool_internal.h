#ifndef OS_TRANSPORT_THREAD_POOL_INTERNAL_H
#define OS_TRANSPORT_THREAD_POOL_INTERNAL_H

#include "os_transport.h"
#include "os_transport_thread_pool.h"
#include <ub/umdk/urma/urma_api.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

// 兼容仍通过内部头文件使用TransportData的调用方；线程池内部统一使用os_transport_user_data_t。
typedef os_transport_user_data_t TransportData;

/**
 * @brief Worker线程状态枚举
 */
typedef enum {
    WORKER_STATE_INIT = 0, // 初始化未运行
    WORKER_STATE_IDLE = 1, // 空闲（已启动，等待任务）
    WORKER_STATE_BUSY = 2, // 忙碌（执行任务）
    WORKER_STATE_EXIT = 3  // 退出
} WorkerState;

// 任务节点
typedef struct TaskNode {
    ThreadPoolTask *task;
    struct TaskNode *next;
} TaskNode;

typedef struct PendingReqNode {
    uint32_t request_id;
    // 每个唤醒事件携带一份user_data副本，避免多个completion覆盖同一request上下文
    TransportData user_data;
    struct PendingReqNode *next;
} PendingReqNode;

/**
 * @brief Worker线程结构体（链表版本）
 */
typedef struct {
    pthread_t tid;                    // 线程ID
    pthread_mutex_t mutex;            // 线程锁
    pthread_cond_t cond_task;         // 任务通知条件变量
    WorkerState state;                // 线程状态
    int worker_idx;                   // 线程索引
    ThreadPoolHandle pool;            // 所属线程池句柄
    TaskNode *queue_head;             // 队首
    TaskNode *queue_tail;             // 队尾
    uint32_t queue_size;              // 当前任务数
    PendingReqNode *pending_req_head; // 待执行 request_id/user_data 队首
    PendingReqNode *pending_req_tail; // 待执行 request_id/user_data 队尾
    uint32_t pending_req_count;       // 等待执行的 request_id 数量
} WorkerThread;

/**
 * @brief 请求上下文（用于记录request_id与worker的绑定及批次信息）
 */
typedef struct RequestContext {
    uint32_t request_id;
    int worker_idx;          // 绑定的 worker 索引
    int pending_count;       // 剩余任务数
    TaskCompleteCb batch_cb; // 批次完成回调
    void *batch_user_data;
    struct RequestContext *next; // 哈希冲突链表
} RequestContext;

/**
 * @brief urma相关信息，用于与asyncPool线程绑定
 */
typedef struct {
    urma_jfce_t *jfce;
    urma_jfc_t *jfc;
    bool urma_event_mode;
} ThreadPoolUrmaInfo;

#define REQ_HASH_SIZE 1024
#define EPOLL_TIME    100
#define POLL_SIZE     10
#define POLL_TRY_CNT  10

/**
 * @brief 线程池内部结构
 */
struct _ThreadPool {
    // 线程基础配置
    WorkerThread *workers; // Worker线程数组
    uint32_t worker_count; // Worker线程数量
    bool is_running;       // 线程池运行标记
    bool is_destroying;    // 销毁标记

    // 任务ID生成
    uint64_t next_task_id;         // 下一个任务ID
    pthread_mutex_t task_id_mutex; // 任务ID锁

    // 中断&同步控制
    pthread_mutex_t global_mutex; // 全局锁（用于通知队列等）

    // 线程启动同步
    pthread_cond_t cond_start;   // 线程启动信号
    pthread_mutex_t start_mutex; // 启动控制锁

    // URMA 相关信息
    ThreadPoolUrmaInfo urmaInfo;

    // request_id 哈希表
    RequestContext *req_hash[REQ_HASH_SIZE];
    pthread_mutex_t req_hash_mutex; // 保护哈希表
};

#endif // OS_TRANSPORT_THREAD_POOL_INTERNAL_H

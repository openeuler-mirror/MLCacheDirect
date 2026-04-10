#ifndef OS_TRANSPORT_THREAD_POOL_INTERNAL_H
#define OS_TRANSPORT_THREAD_POOL_INTERNAL_H

#include "os_transport_thread_pool.h"
#include "os_transport_urma.h"
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <syslog.h>

enum {
    OST_SYSLOG_PRIO_DEBUG = LOG_DEBUG,
    OST_SYSLOG_PRIO_INFO = LOG_INFO,
    OST_SYSLOG_PRIO_WARNING = LOG_WARNING,
    OST_SYSLOG_PRIO_ERR = LOG_ERR
};

typedef union {
    struct {
        uint64_t chunk_type : 2;
        uint64_t chunk_id : 6;
        uint64_t chunk_size : 24;
        uint64_t request_id : 32;
    } bs;
    uint64_t user_ctx;
} TransportData;

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

/**
 * @brief Worker线程结构体（链表版本）
 */
typedef struct {
    pthread_t tid;            // 线程ID
    pthread_mutex_t mutex;    // 线程锁
    pthread_cond_t cond_task; // 任务通知条件变量
    WorkerState state;        // 线程状态
    int worker_idx;           // 线程索引
    ThreadPoolHandle pool;    // 所属线程池句柄
    TaskNode *queue_head;     // 队首
    TaskNode *queue_tail;     // 队尾
    uint32_t queue_size;      // 当前任务数
    uint32_t pending_req;     // 等待执行的 request_id
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

// 日志级别
typedef enum { LOG_LEVEL_DEBUG = 0, LOG_LEVEL_INFO, LOG_LEVEL_WARN, LOG_LEVEL_ERROR } LogLevel;

// 全局日志级别控制（可通过编译宏/配置修改）
#ifndef GLOBAL_LOG_LEVEL
#define GLOBAL_LOG_LEVEL LOG_LEVEL_DEBUG
#endif

static inline int log_level_to_syslog_priority(LogLevel level)
{
    switch (level) {
    case LOG_LEVEL_DEBUG:
        return OST_SYSLOG_PRIO_DEBUG;
    case LOG_LEVEL_INFO:
        return OST_SYSLOG_PRIO_INFO;
    case LOG_LEVEL_WARN:
        return OST_SYSLOG_PRIO_WARNING;
    case LOG_LEVEL_ERROR:
        return OST_SYSLOG_PRIO_ERR;
    default:
        return OST_SYSLOG_PRIO_INFO;
    }
}

static inline void init_syslog_logger_once(void)
{
    openlog("os_transport", LOG_PID | LOG_NDELAY, LOG_USER);
}

static inline void log_to_syslog(LogLevel level, const char *file, int line, const char *fmt, ...)
{
    static pthread_once_t log_init_once = PTHREAD_ONCE_INIT;
    pthread_once(&log_init_once, init_syslog_logger_once);

    char msg_buf[1024];
    va_list args;
    va_start(args, fmt);
    vsnprintf(msg_buf, sizeof(msg_buf), fmt, args);
    va_end(args);

    syslog(log_level_to_syslog_priority(level), "[%s:%d] %s", file, line, msg_buf);
}

// 日志格式化输出宏
#define OST_LOG(level, fmt, ...)                                                                                       \
    do {                                                                                                               \
        if (level >= GLOBAL_LOG_LEVEL) {                                                                               \
            log_to_syslog(level, __FILE__, __LINE__, fmt, ##__VA_ARGS__);                                              \
        }                                                                                                              \
    } while (0)

// 快捷日志宏
#define OST_LOG_DEBUG(fmt, ...) OST_LOG(LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__)
#define OST_LOG_INFO(fmt, ...)  OST_LOG(LOG_LEVEL_INFO, fmt, ##__VA_ARGS__)
#define OST_LOG_WARN(fmt, ...)  OST_LOG(LOG_LEVEL_WARN, fmt, ##__VA_ARGS__)
#define OST_LOG_ERROR(fmt, ...) OST_LOG(LOG_LEVEL_ERROR, fmt, ##__VA_ARGS__)

#endif // OS_TRANSPORT_THREAD_POOL_INTERNAL_H

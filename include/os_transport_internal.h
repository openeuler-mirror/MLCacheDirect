#ifndef OS_TRANSPORT_INTERNAL_H
#define OS_TRANSPORT_INTERNAL_H

#include "os_transport.h"
#include "os_transport_thread_pool.h"
#include "os_transport_urma.h"
#include <pthread.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef enum {
    NOT_SPLIT = 0,
    MIDDLE_CHUNK,
    LAST_CHUNK,
} os_transport_chunk_type_t;

typedef struct {
    ThreadPoolTask *tasks;
    void *task_args;
    uint32_t task_num;
} task_group_t;

typedef struct chunk_info {
    uint64_t src; // 源缓冲区地址
    uint64_t dst; // 目标缓冲区地址
    uint32_t len; // 数据长度
} chunk_info_t;

struct task_sync {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int request_completed;    // 该请求的所有task是否都已完成
    uint64_t total_tasks;     // 任务组总任务数
    uint64_t completed_tasks; // 任务组已完成任务数
    task_group_t *task_group; // 任务组，由主线程统一释放
    chunk_info_t *chunks;     // 本次请求关联的chunk数组，由主线程统一释放
};

typedef struct os_transport_handle {
    urma_context_t *urma_ctx;
    uint32_t worker_thread_num;
    bool urma_event_mode;
    ThreadPoolHandle thread_pool;
} os_transport_handle_t;

typedef enum {
    NULL_TASK = 0,
    SEND_TASK,
    RECV_TASK,
} task_type_t;

// send类型的task参数
typedef struct {
    // 与主函数的同步信息
    task_sync_t *sync;
    // chunk相关参数
    chunk_info_t *chunk_info;
    bool is_last_chunk;
    // urma发送端相关参数
    urma_write_info_t write_info;
} send_task_arg_t;

typedef struct {
    // 与主函数的同步信息
    task_sync_t *sync;
    // chunk相关参数
    chunk_info_t *chunk_info;
    bool is_last_chunk;
    // urma接收端相关参数，包括h2d相关信息
    urma_recv_info_t recv_info;
    // 上层注册的notify回调；后续worker应使用wake_up阶段带来的user_data调用它
    notify_callback_t notify_callback;
    // 保存每次wake_up解析出的user_data副本，notify_callback的入参应传该字段地址
    os_transport_user_data_t notify_user_data;
} recv_task_arg_t;

#endif // OS_TRANSPORT_INTERNAL_H

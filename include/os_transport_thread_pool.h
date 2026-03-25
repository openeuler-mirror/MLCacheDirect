#ifndef OS_TRANSPORT_THREAD_POOL_H
#define OS_TRANSPORT_THREAD_POOL_H

#include <stdint.h>
#include <stdbool.h>

/**
 * @brief 任务结构体
 */
typedef struct {
    uint64_t task_id;                 // 唯一任务ID
    uint32_t request_id;              // 请求ID（相同批次任务此值相同）
    int (*task_func)(void* arg);      // 任务执行函数
    void* task_arg;                   // 任务参数（用户自行管理内存）
    bool is_completed;                // 任务完成标记
    bool free_task_self;              // 任务结构体是否由线程池释放
} ThreadPoolTask;

/**
 * @brief 任务完成回调函数
 * @param task_id 任务ID
 * @param success 执行结果（true=成功，false=失败）
 * @param user_data 外部透传数据
 */
typedef void (*TaskCompleteCb)(uint64_t task_id, bool success, void* user_data);

/**
 * @brief 线程池句柄（隐藏内部实现）
 */
typedef struct _ThreadPool* ThreadPoolHandle;

/**
 * @brief 初始化线程池（1个asyncPoll + 64个Worker，仅初始化不运行）
 * @param worker_queue_cap 单个Worker队列容量（建议≥2）
 * @param pending_queue_cap 全局Pending队列初始容量（0=默认1024）（已弃用，保留兼容）
 * @return 线程池句柄（NULL=失败）
 */
ThreadPoolHandle thread_pool_init(uint32_t worker_queue_cap, uint32_t pending_queue_cap);

/**
 * @brief 启动线程池（仅启动asyncPoll，Worker按需唤醒）
 * @param handle 线程池句柄
 * @return 0=成功，-1=失败
 */
int thread_pool_start(ThreadPoolHandle handle);

/**
 * @brief 外部提交单个任务
 * @param handle 线程池句柄
 * @param request_id 请求ID
 * @param task_func 任务执行函数
 * @param task_arg 任务参数
 * @param complete_cb 任务完成回调
 * @param user_data 回调透传数据
 * @return 任务ID（0=失败）
 */
uint64_t thread_pool_submit_task(ThreadPoolHandle handle,
                                 uint32_t request_id,
                                 int (*task_func)(void* arg),
                                 void* task_arg,
                                 TaskCompleteCb complete_cb,
                                 void* user_data);

/**
 * @brief 批量提交任务（所有任务入同一个worker线程，保证执行顺序）
 * @param handle 线程池句柄
 * @param tasks 任务数组（用户需分配，每个任务的task_func必须有效）
 * @param task_count 任务数量
 * @param complete_cb 任务完成回调（每个任务完成时调用）
 * @param user_data 回调透传数据（传递给complete_cb）
 * @param batch_complete_cb 批量任务全部完成回调
 * @param batch_user_data 批量任务全部完成回调透传数据
 * @return 任务ID数组（长度=task_count，NULL=失败，用户需自行释放）
 */
uint64_t* thread_pool_submit_batch_tasks(ThreadPoolHandle handle,
                                         ThreadPoolTask* tasks,
                                         uint32_t task_count,
                                         TaskCompleteCb complete_cb,
                                         void* user_data,
                                         TaskCompleteCb batch_complete_cb,
                                         void* batch_user_data);


/**
 * @brief 根据 request_id 销毁所有未执行的任务
 * @param handle 线程池句柄
 * @param request_id 请求ID
 * @return 销毁的任务数量，-1表示参数错误
 */
int thread_pool_cancel_tasks_by_req(ThreadPoolHandle handle, uint32_t request_id);

/**
 * @brief 销毁线程池（等待所有任务完成，释放资源）
 * @param handle 线程池句柄
 */
void thread_pool_destroy(ThreadPoolHandle handle);

#endif // OS_TRANSPORT_THREAD_POOL_H
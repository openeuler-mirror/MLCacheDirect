/**
 * @file os_transport_inject.h
 * @brief os_transport 层故障注入机制（跨进程共享内存版）
 *
 * 支持多进程共享故障注入配置，适用于测试工具控制 Worker 进程的场景。
 * 编译时添加 -DOS_TRANSPORT_WITH_INJECT=1 启用。
 */

#ifndef OS_TRANSPORT_INJECT_H
#define OS_TRANSPORT_INJECT_H

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/types.h> /* for pid_t */

#ifdef __cplusplus
extern "C" {
#endif

/* 共享内存配置 */
#define OS_TRANSPORT_INJECT_SHM_NAME "/os_transport_inject"
#define OS_TRANSPORT_INJECT_SHM_SIZE 65536
#define OS_TRANSPORT_INJECT_VERSION  0x20250413 // 版本号，格式：YYYYMMDD

/* 注入点配置 */
#define OS_TRANSPORT_MAX_INJECT_POINTS     32
#define OS_TRANSPORT_INJECT_NAME_MAX_LEN   64
#define OS_TRANSPORT_INJECT_ACTION_MAX_LEN 128

/* 注入动作类型 */
typedef enum {
    OS_TRANSPORT_INJECT_ACTION_NONE = 0,
    OS_TRANSPORT_INJECT_ACTION_RETURN_ERROR,
    OS_TRANSPORT_INJECT_ACTION_SLEEP,
    OS_TRANSPORT_INJECT_ACTION_PRINT,
} os_transport_inject_action_type_t;

/* 注入点结构（注意：无指针，纯数据，可安全放入共享内存） */
typedef struct {
    char name[OS_TRANSPORT_INJECT_NAME_MAX_LEN];
    os_transport_inject_action_type_t action_type;
    int return_value;
    uint32_t sleep_ms;
    char message[OS_TRANSPORT_INJECT_ACTION_MAX_LEN];
    volatile int enabled; // 使用 volatile + 原子操作
    uint32_t frequency_percent;
    volatile uint32_t trigger_count;
    uint32_t trigger_limit;
    char _padding[32]; // 防止伪共享
} os_transport_inject_point_t;

/* 共享内存注册表头 */
typedef struct {
    volatile uint32_t version; // 版本号，用于检测兼容性
    volatile uint32_t seq;     // 序列号，用于检测变更
    volatile int lock;         // 自旋锁（0=未锁，1=已锁）
    volatile uint32_t count;
    volatile pid_t writer_pid; // 最后写入者（调试用）
} os_transport_inject_shm_header_t;

/* 触发记录（用于 verify 防伪） */
#define OS_TRANSPORT_INJECT_RING_SIZE 64

typedef struct {
    uint64_t timestamp_ns;
    char name[OS_TRANSPORT_INJECT_NAME_MAX_LEN];
    int action_type;
    int return_value;
} inject_record_t;

/* 完整注册表（共享内存布局） */
typedef struct {
    os_transport_inject_shm_header_t header;
    os_transport_inject_point_t points[OS_TRANSPORT_MAX_INJECT_POINTS];
    volatile uint32_t ring_tail;
    char _ring_padding[60]; /* pad to 64 bytes */
    inject_record_t ring[OS_TRANSPORT_INJECT_RING_SIZE];
} os_transport_inject_registry_t;

/* 四个关键流程的注入点名称 */
#define OS_TRANSPORT_INJECT_RECV_BEGIN      "os_transport.recv.begin"
#define OS_TRANSPORT_INJECT_SEND_BEGIN      "os_transport.send.begin"
#define OS_TRANSPORT_INJECT_URMA_WRITE      "os_transport.urma.write"
#define OS_TRANSPORT_INJECT_NOTIFY_CALLBACK "os_transport.notify_callback"

/* 补充注入点名称（覆盖内部错误分支） */
#define OS_TRANSPORT_INJECT_URMA_RECV        "os_transport.urma.recv"
#define OS_TRANSPORT_INJECT_SEND_FIRST_CHUNK "os_transport.send.first_chunk"
#define OS_TRANSPORT_INJECT_ALLOC_TASK_GROUP "os_transport.task_group.alloc"

#if defined(OS_TRANSPORT_WITH_INJECT) && OS_TRANSPORT_WITH_INJECT

/* 初始化并获取共享内存注册表（每个进程自动调用） */
os_transport_inject_registry_t *os_transport_inject_init(void);

/* API：设置注入点（线程安全，跨进程可见） */
int os_transport_inject_set_sleep(const char *name, uint32_t ms);
int os_transport_inject_set_return_error(const char *name, int error_code);
int os_transport_inject_set_print(const char *name, const char *message);
int os_transport_inject_set_frequency(const char *name, uint32_t percent_or_count);

/* API：清除注入点 */
void os_transport_inject_clear(const char *name);
void os_transport_inject_clear_all(void);

/* API：执行注入点（内部使用） */
int os_transport_inject_execute(const char *name, int *out_return_value);

/* API：强制清理共享内存（管理员/测试后使用） */
void os_transport_inject_cleanup(void);

/* API：获取当前注入点数量（调试用） */
uint32_t os_transport_inject_get_count(void);

/* 注入宏 */
#define OS_TRANSPORT_INJECT_POINT(name, ret_var)                                                                       \
    do {                                                                                                               \
        int __inject_ret = 0;                                                                                          \
        if (os_transport_inject_execute(name, &__inject_ret)) {                                                        \
            ret_var = (typeof(ret_var))__inject_ret;                                                                   \
            fprintf(stderr,                                                                                            \
                    "[OS_TRANSPORT_INJECT] Injected at %s:%d (%s), ret=%d\n",                                          \
                    __FILE__,                                                                                          \
                    __LINE__,                                                                                          \
                    name,                                                                                              \
                    __inject_ret);                                                                                     \
            return ret_var;                                                                                            \
        }                                                                                                              \
    } while (0)

#define OS_TRANSPORT_INJECT_POINT_VOID(name)                                                                           \
    do {                                                                                                               \
        int __inject_ret = 0;                                                                                          \
        os_transport_inject_execute(name, &__inject_ret);                                                              \
    } while (0)

#else /* OS_TRANSPORT_WITH_INJECT */

/* 空实现 */
static inline int os_transport_inject_set_sleep(const char *n, uint32_t m)
{
    (void)n;
    (void)m;
    return 0;
}
static inline int os_transport_inject_set_return_error(const char *n, int c)
{
    (void)n;
    (void)c;
    return 0;
}
static inline int os_transport_inject_set_print(const char *n, const char *m)
{
    (void)n;
    (void)m;
    return 0;
}
static inline int os_transport_inject_set_frequency(const char *n, uint32_t p)
{
    (void)n;
    (void)p;
    return 0;
}
static inline void os_transport_inject_clear(const char *n)
{
    (void)n;
}
static inline void os_transport_inject_clear_all(void)
{}
static inline void os_transport_inject_cleanup(void)
{}
static inline uint32_t os_transport_inject_get_count(void)
{
    return 0;
}
static inline int os_transport_inject_execute(const char *n, int *r)
{
    (void)n;
    (void)r;
    return 0;
}

#define OS_TRANSPORT_INJECT_POINT(name, ret_var) ((void)0)
#define OS_TRANSPORT_INJECT_POINT_VOID(name)     ((void)0)

#endif /* OS_TRANSPORT_WITH_INJECT */

#ifdef __cplusplus
}
#endif

#endif /* OS_TRANSPORT_INJECT_H */

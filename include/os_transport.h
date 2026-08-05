#ifndef OS_TRANSPORT_H
#define OS_TRANSPORT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#ifdef USE_URMA_MOCK
#include "urma_abi_compat_extend.h"
#else
#include <ub/umdk/urma/urma_api.h>
#ifdef URMA_OVER_UB
#include <ub/umdk/urma/urma_ubagg.h>
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef void (*log_callback_t)(int level, const char *msg);
int os_transport_log_reg(int level, log_callback_t cb);

/*
 * recv notify回调。user_data指针指向每次URMA completion对应的
 * os_transport_user_data_t副本，该副本在os_transport_wake_up_task阶段填充。
 */
typedef int (*notify_callback_t)(void *user_data);

#define DEFAULT_CHUNK_SIZE          (2 * 1024 * 1024) // 2MB
#define REQUEST_ID_BITS             60U               // request_id传输字段占用的位数
#define REQUEST_ID_EFFECTIVE_BITS   40U               // API传入的request_id仅低40位有效
#define CHUNK_TYPE_BITS             1                 // chunk_type占用的位数
#define CHUNK_ID_BITS               3                 // chunk_id占用的位数

#define OS_TRANSPORT_MAX_CHUNK_ID  ((1ULL << CHUNK_ID_BITS) - 2)
#define OS_TRANSPORT_MAX_CHUNK_NUM ((uint32_t)(OS_TRANSPORT_MAX_CHUNK_ID + 1))

typedef union {
    struct {
        uint64_t request_id : REQUEST_ID_BITS; // 实际可用40位；API使用连续低40位，内部将WRITE_WITH_IMM
                                               // 的request_id编码到传输字段的[59:40]和[19:0]。
        uint64_t chunk_type : CHUNK_TYPE_BITS; // 1位：0表示非尾片，1表示尾片
        uint64_t chunk_id : CHUNK_ID_BITS;     // 3位：最多支持8个分片
    } bs;
    uint64_t user_ctx;
} os_transport_user_data_t;

typedef struct {
    uint64_t addr;           // 数据缓冲区地址
    urma_target_seg_t *tseg; // 目标分段信息
} ost_buffer_info_t;

typedef struct {
    urma_jfr_t *jfr;     // 兼容旧版本：用于接收的JFR
    urma_jetty_t *jetty; // 最新版本：用于接收的Jetty，优先使用urma_post_jetty_recv_wr
    void *dst;           // 数据缓冲区地址
} ost_device_info_t;

typedef enum jetty_mode { JETTY_MODE_SIMPLEX = 0, JETTY_MODE_DUPLEX } jetty_mode_t;

typedef struct urma_jetty_info {
    urma_jfs_t *jfs;             /* [Public] see urma_jetty_info. */
    urma_jetty_t *jetty;         /* [Public] see urma_jetty_info. */
    urma_target_jetty_t *tjetty; /* [Public] see urma_jetty_info. */
    jetty_mode_t jetty_mode;     /* [Public] see urma_jetty_info. */
} urma_jetty_info_t;

typedef struct os_transport_cfg {
    bool urma_event_mode;
    uint8_t reserved1[3];         // 保留字节，保持结构体对齐
    uint32_t worker_thread_num;   // 线程池中工作线程数量
    urma_jfce_t *jfce;            // 关联的JFCE对象
    urma_jfc_t *jfc;              // 关联的JFC对象
    uint32_t recv_queue_capacity; // 接收队列可用资源初始值
    uint32_t reserved2[9];
} os_transport_cfg_t;

typedef struct task_sync task_sync_t;

uint32_t os_transport_init(urma_context_t *urma_ctx, os_transport_cfg_t *ost_cfg, void **handle);

uint32_t os_transport_reg_jfc(urma_jfce_t *jfce, urma_jfc_t *jfc, void *handle);

uint32_t os_transport_send(void *handle,
                           urma_jetty_info_t *jetty_info,
                           ost_buffer_info_t *local_src,
                           ost_buffer_info_t *remote_dst,
                           uint32_t len,
                           uint64_t server_key,
                           uint64_t client_key,
                           task_sync_t **ret_sync_handle,
                           urma_status_t *urma_status);

uint32_t os_transport_recv(void *handle,
                           ost_buffer_info_t *host_src,
                           ost_device_info_t *device_dst,
                           uint32_t len,
                           uint64_t client_key,
                           task_sync_t **ret_sync_handle,
                           notify_callback_t notify_callback);

int os_transport_wake_up_task(void *handle, void *cr_t);

/*
 * urma_status is optional. For a send request, it receives the first
 * non-success status from an asynchronous urma_write_with_notify() call.
 * A non-zero API return with urma_status == URMA_SUCCESS indicates a transport
 * library error rather than a task-function error.
 */
uint32_t wait_and_free_sync(void *handle, task_sync_t *sync_handle, urma_status_t *urma_status);

/*
 * Wait for a split request to finish, but only up to timeout_ms milliseconds.
 * If timeout_ms < 0, it behaves the same as wait_and_free_sync().
 * On timeout, queued tasks for the same request_id are canceled and the sync
 * resources are released immediately when it is safe to do so. Running tasks,
 * if any, are allowed to finish and release the resources themselves.
 */
uint32_t
wait_and_free_sync_timeout(void *handle, task_sync_t *sync_handle, int64_t timeout_ms, urma_status_t *urma_status);

uint32_t os_transport_cancel_tasks(void *handle, task_sync_t **sync_handle, uint64_t request_id);

uint32_t os_transport_destroy(void *handle);

#ifdef __cplusplus
}
#endif
#endif // OS_TRANSPORT_H

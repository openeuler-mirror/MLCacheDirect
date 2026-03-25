#ifndef OS_TRANSPORT_H
#define OS_TRANSPORT_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>
#include <cuda_runtime.h>
#include <ub/umdk/urma/urma_api.h>
#ifdef URMA_OVER_UB
#    include <ub/umdk/urma/urma_ubagg.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif
#define DEFAULT_CHUNK_SIZE (2 * 1024 * 1024)   // 2MB

typedef union {
    struct {
        uint64_t chunk_type : 2;
        uint64_t chunk_id : 6;
        uint64_t chunk_size : 24;
        uint64_t request_id : 32;
    } bs;
    uint64_t user_ctx;
} os_transport_user_data_t;

typedef struct {
    uint64_t addr;             // 数据缓冲区地址
    urma_target_seg_t *tseg;   // 目标分段信息
} ost_buffer_info_t;

typedef struct {
    void *dst;             // 设备地址
    cudaStream_t stream;   // CUDA流
    cudaEvent_t event;     // CUDA事件
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
    uint32_t reserved2[10];
} os_transport_cfg_t;

typedef struct task_sync task_sync_t;

uint32_t os_transport_init(urma_context_t *urma_ctx, os_transport_cfg_t *ost_cfg, void **handle);

uint32_t os_transport_reg_jfc(urma_jfce_t *jfce, urma_jfc_t *jfc, void *handle);

uint32_t os_transport_send(void *handle, urma_jetty_info_t *jetty_info,
                           ost_buffer_info_t *local_src, ost_buffer_info_t *remote_dst,
                           uint32_t len, uint32_t server_key, uint32_t client_key,
                           task_sync_t **ret_sync_handle);

uint32_t os_transport_recv(void *handle, ost_buffer_info_t *host_src, ost_device_info_t *device_dst,
                           uint32_t len, uint32_t client_key, task_sync_t **ret_sync_handle);

uint32_t wait_and_free_sync(void *handle, task_sync_t *sync_handle);

uint32_t os_transport_destroy(void *handle);

#ifdef __cplusplus
}
#endif
#endif   // OS_TRANSPORT_H

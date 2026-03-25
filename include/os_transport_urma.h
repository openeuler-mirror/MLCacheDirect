#ifndef OS_TRANSPORT_URMA_H
#define OS_TRANSPORT_URMA_H

#include "os_transport.h"
#include <stddef.h>
#include <stdint.h>

struct chunk_info;

typedef struct {
    urma_jfs_t *jfs;
    urma_jetty_t *jetty;
    urma_target_jetty_t *target_jfr;
    urma_target_seg_t *dst_tseg;
    urma_target_seg_t *src_tseg;
    urma_jfs_wr_flag_t flag;
    os_transport_user_data_t user_ctx_server;
    os_transport_user_data_t user_ctx_client;
} urma_write_info_t;

typedef struct {
    ost_device_info_t device_info; // 设备信息
    uint32_t request_id;       // 请求ID
} urma_recv_info_t;

typedef union {
    urma_write_info_t write_info;
    urma_recv_info_t recv_info;
} urma_info_t;

urma_status_t urma_write_with_notify(urma_write_info_t write_info, struct chunk_info *chunk_info);
#endif   // OS_TRANSPORT_URMA_H

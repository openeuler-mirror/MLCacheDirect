#include "os_transport_internal.h"

urma_status_t urma_write_with_notify(urma_write_info_t write_info, chunk_info_t *chunk_info)
{
    urma_sge_t src_sge = {
        .addr = chunk_info->src,
        .len = chunk_info->len,
        .tseg = write_info.src_tseg,
    };
    urma_sge_t dst_sge = {
        .addr = chunk_info->dst,
        .len = chunk_info->len,
        .tseg = write_info.dst_tseg,
    };
    urma_sg_t src_sg = {.sge = &src_sge, .num_sge = 1};
    urma_sg_t dst_sg = {.sge = &dst_sge, .num_sge = 1};
    urma_rw_wr_t rw = {
        .src = src_sg,
        .notify_data =
            write_info.user_ctx_client.user_ctx, // 将client_key作为notify_data传入，方便worker线程回调时区分不同请求
        .dst = dst_sg};
    urma_jfs_wr_t wr = {
        .opcode = URMA_OPC_WRITE_NOTIFY,
        .flag.bs.complete_enable = 1,
        .flag.bs.inline_flag = 0,
        .tjetty = write_info.target_jfr,
        .user_ctx =
            write_info.user_ctx_server.user_ctx, // 将server_key作为user_ctx传入，方便worker线程回调时区分不同请求
        .rw = rw,
        .next = NULL};
    urma_jfs_wr_t *bad_wr;
    if (write_info.jfs)
        return urma_post_jfs_wr(write_info.jfs, &wr, &bad_wr);
    else if (write_info.jetty)
        return urma_post_jetty_send_wr(write_info.jetty, &wr, &bad_wr);
    else
        return URMA_FAIL;
}

urma_status_t urma_recv_with_notify(urma_recv_info_t recv_info, chunk_info_t *chunk_info)
{
    urma_sge_t src_sge = {.addr = (uint64_t)chunk_info->src, .len = chunk_info->len, .tseg = recv_info.local_tseg};
    urma_sg_t src_sg = {.sge = &src_sge, .num_sge = 1};
    urma_jfr_wr_t wr = {.src = src_sg,
                        .user_ctx = recv_info.request_id, // TODO：此处的user_ctx有什么作用？
                        .next = NULL};
    urma_jfr_wr_t *bad_wr;

    if (recv_info.jfr)
        // 目前先只实现jfr的recv接口，后续如果jetty也需要支持再加
        return urma_post_jfr_wr(recv_info.jfr, &wr, &bad_wr);
    else
        return URMA_FAIL;
}

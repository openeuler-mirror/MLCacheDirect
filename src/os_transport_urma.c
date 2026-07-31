#include "os_transport_internal.h"
#include "os_transport_log_internal.h"
#if defined(OS_TRANSPORT_WITH_INJECT) && OS_TRANSPORT_WITH_INJECT
#include "os_transport_inject.h"
#endif

static urma_status_t urma_write_internal(urma_write_info_t write_info,
                                         struct chunk_info *chunk_info, urma_opcode_t opcode)
{
#if defined(OS_TRANSPORT_WITH_INJECT) && OS_TRANSPORT_WITH_INJECT
    urma_status_t inject_ret = URMA_SUCCESS;
    OS_TRANSPORT_INJECT_POINT(OS_TRANSPORT_INJECT_URMA_WRITE, inject_ret);
#endif

    urma_status_t ret;
    urma_sge_t src_sge = {0}, dst_sge = {0};
    urma_sg_t src_sg = {0}, dst_sg = {0};
    urma_jfs_wr_t *bad_wr;
    urma_jfs_wr_t wr = {
        .opcode = opcode,
        .flag.bs.complete_enable = 1,
        .flag.bs.inline_flag = 0,
        .tjetty = write_info.target_jfr,
        // 将server_key作为user_ctx传入，方便worker线程回调时区分不同请求
        .user_ctx = write_info.user_ctx_server.user_ctx,
        .next = NULL,
    };
    if (opcode == URMA_OPC_WRITE) {
        if (!chunk_info) {
            OST_LOG_ERROR("Failed: chunk_info is NULL in urma_write_internal.");
            return URMA_FAIL;
        }

        src_sge.addr = chunk_info->src,
        src_sge.len  = chunk_info->len,
        src_sge.tseg = write_info.src_tseg,

        dst_sge.addr = chunk_info->dst,
        dst_sge.len = chunk_info->len,
        dst_sge.tseg = write_info.dst_tseg,

        src_sg.sge = &src_sge;
        src_sg.num_sge = 1;
        dst_sg.sge = &dst_sge;
        dst_sg.num_sge = 1;

        wr.rw.src = src_sg;
        wr.rw.notify_data = 0;
        wr.rw.dst = dst_sg;
    } else if (opcode == URMA_OPC_SEND_IMM) {
        wr.send.src = src_sg;
        // 将client_key作为notify_data传入，方便worker线程回调时区分不同请求
        wr.send.imm_data = write_info.user_ctx_client.user_ctx;
        wr.send.tseg = NULL;

        // set chunk_id invalid to ignore urma_cr_t this time.
        uint64_t invalid_chunk_id = OS_TRANSPORT_MAX_CHUNK_NUM;
        os_transport_user_data_t *user_ctx_server = (os_transport_user_data_t *)&wr.user_ctx;
        user_ctx_server->bs.chunk_id = invalid_chunk_id;
    }

    if (write_info.jfs) {
        ret = urma_post_jfs_wr(write_info.jfs, &wr, &bad_wr);
    } else if (write_info.jetty) {
        ret = urma_post_jetty_send_wr(write_info.jetty, &wr, &bad_wr);
    } else {
        OST_LOG_ERROR("Failed: neither jfs nor jetty is available for write request "
                      "(request_id=%lu, chunk_id=%u, len=%u).",
                      write_info.user_ctx_client.bs.request_id,
                      write_info.user_ctx_client.bs.chunk_id,
                      chunk_info ? chunk_info->len : 0);
        return URMA_FAIL;
    }

    if (ret != URMA_SUCCESS) {
        OST_LOG_ERROR("Failed: URMA write post returned %d "
                      "(request_id=%lu, chunk_id=%u, len=%u).",
                      (int)ret,
                      write_info.user_ctx_client.bs.request_id,
                      write_info.user_ctx_client.bs.chunk_id,
                      chunk_info ? chunk_info->len : 0);
    }

    return ret;
}

urma_status_t urma_write_chunk(urma_write_info_t write_info, struct chunk_info *chunk_info)
{
    return urma_write_internal(write_info, chunk_info, URMA_OPC_WRITE);
}

urma_status_t urma_write_notify(urma_write_info_t write_info, struct chunk_info *chunk_info)
{
    return urma_write_internal(write_info, chunk_info, URMA_OPC_SEND_IMM);
}

urma_status_t urma_recv_with_notify(urma_recv_info_t recv_info, struct chunk_info *chunk_info)
{
    urma_status_t ret;

    if (!chunk_info) {
        OST_LOG_ERROR("Failed: chunk_info is NULL in urma_recv_with_notify.");
        return URMA_FAIL;
    }
#if defined(OS_TRANSPORT_WITH_INJECT) && OS_TRANSPORT_WITH_INJECT
    urma_status_t inject_ret = URMA_SUCCESS;
    OS_TRANSPORT_INJECT_POINT(OS_TRANSPORT_INJECT_URMA_RECV, inject_ret);
#endif
    urma_sge_t src_sge = {.addr = (uint64_t)chunk_info->src, .len = chunk_info->len, .tseg = recv_info.local_tseg};
    urma_sg_t src_sg = {.sge = &src_sge, .num_sge = 1};
    urma_jfr_wr_t wr = {.src = src_sg, .user_ctx = recv_info.request_id, .next = NULL};
    urma_jfr_wr_t *bad_wr = NULL;

    if (recv_info.jetty) {
        ret = urma_post_jetty_recv_wr(recv_info.jetty, &wr, &bad_wr);
    } else if (recv_info.jfr) {
        ret = urma_post_jfr_wr(recv_info.jfr, &wr, &bad_wr);
    } else {
        OST_LOG_ERROR("Failed: neither jetty nor jfr is available for recv request "
                      "(request_id=%lu, len=%u).",
                      recv_info.request_id,
                      chunk_info->len);
        return URMA_FAIL;
    }

    if (ret != URMA_SUCCESS) {
        OST_LOG_ERROR("Failed: URMA recv post returned %d via %s (request_id=%lu, len=%u).",
                      (int)ret,
                      recv_info.jetty ? "jetty" : "jfr",
                      recv_info.request_id,
                      chunk_info->len);
    }
    return ret;
}

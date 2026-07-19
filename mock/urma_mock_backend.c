#include "os_transport_internal.h"
#include "os_transport_urma.h"
#include <string.h>

#ifndef URMA_OPC_WRITE_IMM
#define URMA_OPC_WRITE_IMM 0
#endif

#ifndef URMA_FAIL
#define URMA_FAIL URMA_ERROR
#endif

static pthread_mutex_t mock_mutex = PTHREAD_MUTEX_INITIALIZER;
static int mock_fail_write = 0;
static int mock_fail_recv = 0;

void os_transport_mock_set_fail_write(int fail)
{
    pthread_mutex_lock(&mock_mutex);
    mock_fail_write = fail;
    pthread_mutex_unlock(&mock_mutex);
}

void os_transport_mock_set_fail_recv(int fail)
{
    pthread_mutex_lock(&mock_mutex);
    mock_fail_recv = fail;
    pthread_mutex_unlock(&mock_mutex);
}

urma_status_t urma_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    (void)jfs;
    (void)wr;
    (void)bad_wr;

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_write;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        return URMA_E_FAIL;
    }

    if (wr->rw.dst.sge && wr->rw.src.sge) {
        memcpy((void *)(uintptr_t)wr->rw.dst.sge->addr,
               (void *)(uintptr_t)wr->rw.src.sge->addr,
               wr->rw.src.sge->len);
    }

    return URMA_SUCCESS;
}

urma_status_t urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    (void)jetty;
    (void)wr;
    (void)bad_wr;

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_write;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        return URMA_E_FAIL;
    }

    if (wr->rw.dst.sge && wr->rw.src.sge) {
        memcpy((void *)(uintptr_t)wr->rw.dst.sge->addr,
               (void *)(uintptr_t)wr->rw.src.sge->addr,
               wr->rw.src.sge->len);
    }

    return URMA_SUCCESS;
}

urma_status_t urma_post_jetty_recv_wr(urma_jetty_t *jetty, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr)
{
    (void)jetty;
    (void)wr;
    (void)bad_wr;

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_recv;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        return URMA_E_FAIL;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_post_jfr_wr(urma_jfr_t *jfr, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr)
{
    (void)jfr;
    (void)wr;
    (void)bad_wr;

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_recv;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        return URMA_E_FAIL;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_init(const char *name)
{
    (void)name;
    return URMA_SUCCESS;
}

urma_status_t urma_uninit(void)
{
    return URMA_SUCCESS;
}

urma_status_t urma_query_device(urma_device_t **device_list, int *device_count)
{
    (void)device_list;
    (void)device_count;
    return URMA_SUCCESS;
}

urma_status_t urma_query_device_attr(urma_device_t *device, urma_device_attr_t *attr)
{
    (void)device;
    (void)attr;
    return URMA_SUCCESS;
}

urma_status_t urma_create_context(urma_device_t *device, uint32_t eid_index, urma_context_t **context)
{
    (void)device;
    (void)eid_index;
    (void)context;
    return URMA_SUCCESS;
}

urma_status_t urma_destroy_context(urma_context_t *context)
{
    (void)context;
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfce(urma_context_t *context, urma_jfce_t **jfce)
{
    (void)context;
    (void)jfce;
    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfce(urma_jfce_t *jfce)
{
    (void)jfce;
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *cfg, urma_jfc_t **jfc)
{
    (void)context;
    (void)cfg;
    (void)jfc;
    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfc(urma_jfc_t *jfc)
{
    (void)jfc;
    return URMA_SUCCESS;
}

urma_status_t urma_create_jetty(urma_context_t *context, const urma_jetty_cfg_t *cfg, urma_jetty_t **jetty)
{
    (void)context;
    (void)cfg;
    (void)jetty;
    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jetty(urma_jetty_t *jetty)
{
    (void)jetty;
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *cfg, urma_jfr_t **jfr)
{
    (void)context;
    (void)cfg;
    (void)jfr;
    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfr(urma_jfr_t *jfr)
{
    (void)jfr;
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *cfg, urma_jfs_t **jfs)
{
    (void)context;
    (void)cfg;
    (void)jfs;
    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfs(urma_jfs_t *jfs)
{
    (void)jfs;
    return URMA_SUCCESS;
}

urma_status_t urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *cfg, urma_seg_t **seg)
{
    (void)context;
    (void)cfg;
    (void)seg;
    return URMA_SUCCESS;
}

urma_status_t urma_deregister_seg(urma_seg_t *seg)
{
    (void)seg;
    return URMA_SUCCESS;
}

urma_status_t urma_import_seg(urma_context_t *context, urma_seg_t *seg, const void *token, size_t token_size,
                              const urma_import_seg_flag_t *flag, urma_target_seg_t **target_seg)
{
    (void)context;
    (void)seg;
    (void)token;
    (void)token_size;
    (void)flag;
    (void)target_seg;
    return URMA_SUCCESS;
}

urma_status_t urma_release_seg(urma_target_seg_t *target_seg)
{
    (void)target_seg;
    return URMA_SUCCESS;
}

urma_status_t urma_import_jetty(urma_context_t *context, const void *data, size_t data_size,
                                 urma_target_jetty_t **tjetty)
{
    (void)context;
    (void)data;
    (void)data_size;
    (void)tjetty;
    return URMA_SUCCESS;
}

urma_status_t urma_release_jetty(urma_target_jetty_t *tjetty)
{
    (void)tjetty;
    return URMA_SUCCESS;
}

urma_status_t urma_get_jetty_attr(urma_jetty_t *jetty, urma_jetty_attr_t *attr)
{
    (void)jetty;
    (void)attr;
    return URMA_SUCCESS;
}

urma_status_t urma_get_jfs_attr(urma_jfs_t *jfs, urma_jfs_attr_t *attr)
{
    (void)jfs;
    (void)attr;
    return URMA_SUCCESS;
}

urma_status_t urma_get_eid_list(urma_context_t *context, urma_eid_info_t **eid_list, int *eid_count)
{
    (void)context;
    (void)eid_list;
    (void)eid_count;
    return URMA_SUCCESS;
}

int urma_poll(urma_jfc_t *jfc, int timeout_ms)
{
    (void)jfc;
    (void)timeout_ms;
    return 0;
}

urma_status_t urma_get_cr(urma_jfc_t *jfc, urma_cr_t *cr)
{
    (void)jfc;
    (void)cr;
    return URMA_E_AGAIN;
}

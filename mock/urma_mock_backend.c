#include "os_transport_internal.h"
#include "os_transport_urma.h"
#include <stdlib.h>
#include <string.h>
#include <stdio.h>

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
    if (!jfs || !wr) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_INVALID;
    }

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_write;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_FAIL;
    }

    if (wr->rw.dst.sge && wr->rw.src.sge && wr->rw.src.sge->len > 0) {
        memcpy((void *)(uintptr_t)wr->rw.dst.sge->addr,
               (void *)(uintptr_t)wr->rw.src.sge->addr,
               wr->rw.src.sge->len);
    }

    if (bad_wr) {
        *bad_wr = NULL;
    }
    return URMA_SUCCESS;
}

urma_status_t urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr)
{
    if (!jetty || !wr) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_INVALID;
    }

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_write;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_FAIL;
    }

    if (wr->rw.dst.sge && wr->rw.src.sge && wr->rw.src.sge->len > 0) {
        memcpy((void *)(uintptr_t)wr->rw.dst.sge->addr,
               (void *)(uintptr_t)wr->rw.src.sge->addr,
               wr->rw.src.sge->len);
    }

    if (bad_wr) {
        *bad_wr = NULL;
    }
    return URMA_SUCCESS;
}

urma_status_t urma_post_jetty_recv_wr(urma_jetty_t *jetty, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr)
{
    if (!jetty || !wr) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_INVALID;
    }

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_recv;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_FAIL;
    }

    if (bad_wr) {
        *bad_wr = NULL;
    }
    return URMA_SUCCESS;
}

urma_status_t urma_post_jfr_wr(urma_jfr_t *jfr, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr)
{
    if (!jfr || !wr) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_INVALID;
    }

    pthread_mutex_lock(&mock_mutex);
    int fail = mock_fail_recv;
    pthread_mutex_unlock(&mock_mutex);

    if (fail) {
        if (bad_wr) {
            *bad_wr = wr;
        }
        return URMA_E_FAIL;
    }

    if (bad_wr) {
        *bad_wr = NULL;
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
    if (!device_list || !device_count) {
        return URMA_E_INVALID;
    }

    *device_count = 1;
    *device_list = calloc(1, sizeof(urma_device_t));
    if (!*device_list) {
        *device_count = 0;
        return URMA_E_NO_MEM;
    }
    (*device_list)->type = 0;
    (*device_list)->eid_cnt = 1;
    snprintf((*device_list)->name, URMA_DEVICE_NAME_SIZE, "mock_urma_device");

    return URMA_SUCCESS;
}

urma_status_t urma_query_device_attr(urma_device_t *device, urma_device_attr_t *attr)
{
    if (!device || !attr) {
        return URMA_E_INVALID;
    }

    memset(attr, 0, sizeof(*attr));
    attr->dev_type = 0;
    attr->eid_cnt = 1;
    attr->max_jetty = 16;
    attr->max_jfc = 16;
    attr->max_jfr = 16;
    attr->max_send_wr = 1024;
    attr->max_recv_wr = 1024;
    attr->max_send_sge = 4;
    attr->max_recv_sge = 4;
    attr->max_inline_data = 64;
    attr->max_seg_cnt = 1024;
    attr->page_size = 4096;

    attr->dev_cap.max_write_size = 1024 * 1024 * 1024ULL;
    attr->dev_cap.max_read_size = 1024 * 1024 * 1024ULL;
    attr->dev_cap.max_send_wr = 1024;
    attr->dev_cap.max_recv_wr = 1024;
    attr->dev_cap.max_jetty = 16;
    attr->dev_cap.max_jfc = 16;
    attr->dev_cap.max_jfc_depth = 1024;
    attr->dev_cap.max_jfr = 16;
    attr->dev_cap.page_size = 4096;

    return URMA_SUCCESS;
}

urma_status_t urma_create_context(urma_device_t *device, uint32_t eid_index, urma_context_t **context)
{
    if (!device || !context) {
        return URMA_E_INVALID;
    }

    *context = calloc(1, sizeof(urma_context_t));
    if (!*context) {
        return URMA_E_NO_MEM;
    }
    (*context)->async_fd = -1;
    (*context)->dev_type = device->type;
    (*context)->uasid = eid_index;

    return URMA_SUCCESS;
}

urma_status_t urma_destroy_context(urma_context_t *context)
{
    if (!context) {
        return URMA_E_INVALID;
    }

    free(context);
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfce(urma_context_t *context, urma_jfce_t **jfce)
{
    if (!context || !jfce) {
        return URMA_E_INVALID;
    }

    *jfce = calloc(1, sizeof(urma_jfce_t));
    if (!*jfce) {
        return URMA_E_NO_MEM;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfce(urma_jfce_t *jfce)
{
    if (!jfce) {
        return URMA_E_INVALID;
    }

    free(jfce);
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *cfg, urma_jfc_t **jfc)
{
    if (!context || !jfc) {
        return URMA_E_INVALID;
    }

    *jfc = calloc(1, sizeof(urma_jfc_t));
    if (!*jfc) {
        return URMA_E_NO_MEM;
    }

    if (cfg) {
        (*jfc)->depth = cfg->depth;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfc(urma_jfc_t *jfc)
{
    if (!jfc) {
        return URMA_E_INVALID;
    }

    free(jfc);
    return URMA_SUCCESS;
}

urma_status_t urma_create_jetty(urma_context_t *context, const urma_jetty_cfg_t *cfg, urma_jetty_t **jetty)
{
    if (!context || !jetty) {
        return URMA_E_INVALID;
    }

    *jetty = calloc(1, sizeof(urma_jetty_t));
    if (!*jetty) {
        return URMA_E_NO_MEM;
    }

    (*jetty)->state = 1;

    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jetty(urma_jetty_t *jetty)
{
    if (!jetty) {
        return URMA_E_INVALID;
    }

    free(jetty);
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *cfg, urma_jfr_t **jfr)
{
    if (!context || !jfr) {
        return URMA_E_INVALID;
    }

    *jfr = calloc(1, sizeof(urma_jfr_t));
    if (!*jfr) {
        return URMA_E_NO_MEM;
    }

    if (cfg) {
        (*jfr)->depth = cfg->depth;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfr(urma_jfr_t *jfr)
{
    if (!jfr) {
        return URMA_E_INVALID;
    }

    free(jfr);
    return URMA_SUCCESS;
}

urma_status_t urma_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *cfg, urma_jfs_t **jfs)
{
    if (!context || !jfs) {
        return URMA_E_INVALID;
    }

    *jfs = calloc(1, sizeof(urma_jfs_t));
    if (!*jfs) {
        return URMA_E_NO_MEM;
    }

    (*jfs)->state = 1;
    if (cfg) {
        (*jfs)->depth = cfg->depth;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_destroy_jfs(urma_jfs_t *jfs)
{
    if (!jfs) {
        return URMA_E_INVALID;
    }

    free(jfs);
    return URMA_SUCCESS;
}

urma_status_t urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *cfg, urma_seg_t **seg)
{
    if (!context || !cfg || !seg) {
        return URMA_E_INVALID;
    }

    if (cfg->len == 0) {
        return URMA_E_INVALID;
    }

    *seg = calloc(1, sizeof(urma_seg_t));
    if (!*seg) {
        return URMA_E_NO_MEM;
    }

    (*seg)->ubva.va = cfg->va;
    (*seg)->len = cfg->len;
    (*seg)->attr.value = cfg->flag.value;

    return URMA_SUCCESS;
}

urma_status_t urma_deregister_seg(urma_seg_t *seg)
{
    if (!seg) {
        return URMA_E_INVALID;
    }

    free(seg);
    return URMA_SUCCESS;
}

urma_status_t urma_import_seg(urma_context_t *context, urma_seg_t *seg, const void *token, size_t token_size,
                              const urma_import_seg_flag_t *flag, urma_target_seg_t **target_seg)
{
    if (!context || !seg || !target_seg) {
        return URMA_E_INVALID;
    }

    *target_seg = calloc(1, sizeof(urma_target_seg_t));
    if (!*target_seg) {
        return URMA_E_NO_MEM;
    }

    (*target_seg)->seg = *seg;

    return URMA_SUCCESS;
}

urma_status_t urma_release_seg(urma_target_seg_t *target_seg)
{
    if (!target_seg) {
        return URMA_E_INVALID;
    }

    free(target_seg);
    return URMA_SUCCESS;
}

urma_status_t urma_import_jetty(urma_context_t *context, const void *data, size_t data_size,
                                 urma_target_jetty_t **tjetty)
{
    if (!context || !data || !tjetty) {
        return URMA_E_INVALID;
    }

    *tjetty = calloc(1, sizeof(urma_target_jetty_t));
    if (!*tjetty) {
        return URMA_E_NO_MEM;
    }

    return URMA_SUCCESS;
}

urma_status_t urma_release_jetty(urma_target_jetty_t *tjetty)
{
    if (!tjetty) {
        return URMA_E_INVALID;
    }

    free(tjetty);
    return URMA_SUCCESS;
}

urma_status_t urma_get_jetty_attr(urma_jetty_t *jetty, urma_jetty_attr_t *attr)
{
    if (!jetty || !attr) {
        return URMA_E_INVALID;
    }

    memset(attr, 0, sizeof(*attr));
    attr->mask = 1;
    attr->state = jetty->state;

    return URMA_SUCCESS;
}

urma_status_t urma_get_jfs_attr(urma_jfs_t *jfs, urma_jfs_attr_t *attr)
{
    if (!jfs || !attr) {
        return URMA_E_INVALID;
    }

    memset(attr, 0, sizeof(*attr));
    attr->mask = 1;
    attr->state = jfs->state;

    return URMA_SUCCESS;
}

urma_status_t urma_get_eid_list(urma_context_t *context, urma_eid_info_t **eid_list, int *eid_count)
{
    if (!context || !eid_list || !eid_count) {
        return URMA_E_INVALID;
    }

    *eid_count = 1;
    *eid_list = calloc(1, sizeof(urma_eid_info_t));
    if (!*eid_list) {
        *eid_count = 0;
        return URMA_E_NO_MEM;
    }
    (*eid_list)->eid_index = 0;

    return URMA_SUCCESS;
}

int urma_poll(urma_jfc_t *jfc, int timeout_ms)
{
    if (!jfc) {
        return -1;
    }

    (void)timeout_ms;
    return 0;
}

urma_status_t urma_get_cr(urma_jfc_t *jfc, urma_cr_t *cr)
{
    if (!jfc || !cr) {
        return URMA_E_INVALID;
    }

    memset(cr, 0, sizeof(*cr));
    return URMA_E_AGAIN;
}
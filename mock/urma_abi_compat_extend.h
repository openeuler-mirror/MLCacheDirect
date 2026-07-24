/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @brief mock for pipeline rh2d used urma api
 */

#ifndef URMA_ABI_COMPAT_EXTEND_H
#define URMA_ABI_COMPAT_EXTEND_H

#include "urma_abi_compat.h"

#ifdef __cplusplus
extern "C" {
#endif

#define URMA_FAIL URMA_E_FAIL

struct urma_jfr_wr {
    urma_sg_t src;
    uint64_t user_ctx;
    struct urma_jfr_wr *next;
};
typedef struct urma_jfr_wr urma_jfr_wr_t;

urma_status_t urma_init(const char *name);
urma_status_t urma_uninit(void);
urma_status_t urma_query_device(urma_device_t **device_list, int *device_count);
urma_status_t urma_query_device_attr(urma_device_t *device, urma_device_attr_t *attr);
urma_status_t urma_create_context(urma_device_t *device, uint32_t eid_index, urma_context_t **context);
urma_status_t urma_destroy_context(urma_context_t *context);
urma_status_t urma_create_jfce(urma_context_t *context, urma_jfce_t **jfce);
urma_status_t urma_destroy_jfce(urma_jfce_t *jfce);
urma_status_t urma_create_jfc(urma_context_t *context, const urma_jfc_cfg_t *cfg, urma_jfc_t **jfc);
urma_status_t urma_destroy_jfc(urma_jfc_t *jfc);
urma_status_t urma_create_jetty(urma_context_t *context, const urma_jetty_cfg_t *cfg, urma_jetty_t **jetty);
urma_status_t urma_destroy_jetty(urma_jetty_t *jetty);
urma_status_t urma_create_jfr(urma_context_t *context, const urma_jfr_cfg_t *cfg, urma_jfr_t **jfr);
urma_status_t urma_destroy_jfr(urma_jfr_t *jfr);
urma_status_t urma_create_jfs(urma_context_t *context, const urma_jfs_cfg_t *cfg, urma_jfs_t **jfs);
urma_status_t urma_destroy_jfs(urma_jfs_t *jfs);
urma_status_t urma_register_seg(urma_context_t *context, const urma_seg_cfg_t *cfg, urma_seg_t **seg);
urma_status_t urma_deregister_seg(urma_seg_t *seg);
urma_status_t urma_import_seg(urma_context_t *context, urma_seg_t *seg, const void *token, size_t token_size,
                              const urma_import_seg_flag_t *flag, urma_target_seg_t **target_seg);
urma_status_t urma_release_seg(urma_target_seg_t *target_seg);
urma_status_t urma_import_jetty(urma_context_t *context, const void *data, size_t data_size,
                                 urma_target_jetty_t **tjetty);
urma_status_t urma_release_jetty(urma_target_jetty_t *tjetty);
urma_status_t urma_get_jetty_attr(urma_jetty_t *jetty, urma_jetty_attr_t *attr);
urma_status_t urma_get_jfs_attr(urma_jfs_t *jfs, urma_jfs_attr_t *attr);
urma_status_t urma_get_eid_list(urma_context_t *context, urma_eid_info_t **eid_list, int *eid_count);
urma_status_t urma_post_jfs_wr(urma_jfs_t *jfs, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr);
urma_status_t urma_post_jetty_send_wr(urma_jetty_t *jetty, urma_jfs_wr_t *wr, urma_jfs_wr_t **bad_wr);
urma_status_t urma_post_jetty_recv_wr(urma_jetty_t *jetty, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr);
urma_status_t urma_post_jfr_wr(urma_jfr_t *jfr, urma_jfr_wr_t *wr, urma_jfr_wr_t **bad_wr);
int urma_poll(urma_jfc_t *jfc, int timeout_ms);
urma_status_t urma_get_cr(urma_jfc_t *jfc, urma_cr_t *cr);


#ifdef __cplusplus
}
#endif

#endif

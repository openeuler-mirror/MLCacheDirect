#ifndef URMA_ABI_COMPAT_H
#define URMA_ABI_COMPAT_H

#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum urma_status_t {
    URMA_SUCCESS = 0,
    URMA_ERROR = -1,
    URMA_E_FAIL = -1,
    URMA_E_NOT_SUPPORT = -2,
    URMA_E_INVALID = -3,
    URMA_E_BUSY = -4,
    URMA_E_TIMEOUT = -5,
    URMA_E_AGAIN = -6,
    URMA_E_NO_MEM = -7,
    URMA_E_PARTIAL = -8,
    URMA_E_IO = -9,
} urma_status_t;

#define URMA_FAIL URMA_E_FAIL

typedef enum urma_opcode_t {
    URMA_OPC_WRITE = 0,
    URMA_OPC_READ = 1,
    URMA_OPC_SEND = 2,
    URMA_OPC_RECV = 3,
    URMA_OPC_ATOMIC = 4,
    URMA_OPC_BOND_WRITE = 5,
    URMA_OPC_WRITE_IMM = 6,
} urma_opcode_t;

typedef enum urma_cr_status_t {
    URMA_CR_SUCCESS = 0,
    URMA_CR_REM_ACCESS_ABORT_ERR = 1,
    URMA_CR_REM_FLUSH_ERR = 2,
    URMA_CR_REM_INVALID_REQ_ERR = 3,
    URMA_CR_LOC_ACCESS_ERR = 4,
    URMA_CR_LOC_LEN_ERR = 5,
    URMA_CR_LOC_PROT_ERR = 6,
    URMA_CR_LOC_QP_ERR = 7,
    URMA_CR_WR_FLUSH_ERR_DONE = 8,
    URMA_CR_GENERAL_ERR = 9,
} urma_cr_status_t;

typedef enum urma_cr_opcode_t {
    URMA_CR_OPC_WRITE_WITH_IMM = 0,
    URMA_CR_OPC_SEND = 1,
    URMA_CR_OPC_READ = 2,
    URMA_CR_OPC_SEND_WITH_IMM = 3,
} urma_cr_opcode_t;

#ifndef URMA_EID_SIZE
#define URMA_EID_SIZE 16
#endif
#ifndef URMA_TOKEN_SIZE
#define URMA_TOKEN_SIZE 64
#endif
#ifndef URMA_EID_INFO_RESERVED_SIZE
#define URMA_EID_INFO_RESERVED_SIZE 60
#endif
#ifndef URMA_DEVICE_NAME_SIZE
#define URMA_DEVICE_NAME_SIZE 64
#endif
#ifndef URMA_RJETTY_DATA_SIZE
#define URMA_RJETTY_DATA_SIZE 64
#endif

typedef struct urma_eid {
    uint8_t raw[URMA_EID_SIZE];
} urma_eid_t;

struct urma_obj_id {
    urma_eid_t eid;
    uint32_t uasid;
    uint64_t id;
};
typedef struct urma_obj_id urma_obj_id_t;

typedef struct urma_jfce urma_jfce_t;
typedef struct urma_target_jetty urma_target_jetty_t;
typedef struct urma_target_seg urma_target_seg_t;

struct urma_jfce {
    urma_obj_id_t jfce_id;
    void *priv;
};

struct urma_target_jetty {
    urma_obj_id_t jetty_id;
    void *priv;
};

struct urma_jetty {
    urma_obj_id_t jetty_id;
    uint32_t state;
    void *priv;
};
typedef struct urma_jetty urma_jetty_t;

struct urma_jfc {
    urma_obj_id_t jfc_id;
    uint32_t depth;
    void *priv;
};
typedef struct urma_jfc urma_jfc_t;

struct urma_jfr {
    urma_obj_id_t jfr_id;
    uint32_t depth;
    void *priv;
};
typedef struct urma_jfr urma_jfr_t;

struct urma_jfs {
    union {
        urma_obj_id_t jfs_id;
        urma_obj_id_t jetty_id;
    };
    uint32_t state;
    uint32_t depth;
    void *priv;
};
typedef struct urma_jfs urma_jfs_t;

struct urma_context {
    int async_fd;
    int dev_type;
    urma_eid_t eid;
    uint32_t uasid;
};
typedef struct urma_context urma_context_t;

struct urma_device {
    int type;
    int eid_cnt;
    char name[URMA_DEVICE_NAME_SIZE];
};
typedef struct urma_device urma_device_t;

typedef uint64_t urma_addr_t;

struct urma_sge {
    urma_addr_t addr;
    uint32_t len;
    uint32_t lkey;
    uint32_t rkey;
    urma_target_seg_t *tseg;
    void *user_tseg;
};
typedef struct urma_sge urma_sge_t;

struct urma_sg {
    urma_sge_t *sge;
    uint32_t num_sge;
};
typedef struct urma_sg urma_sg_t;

struct urma_rw_wr_t {
    urma_sg_t src;
    urma_sg_t dst;
    uint64_t target_hint;
    uint64_t notify_data;
};
typedef struct urma_rw_wr_t urma_rw_wr_t;

struct urma_jfs_wr_flag_t {
    union {
        struct {
            uint32_t complete_enable : 1;
            uint32_t inline_flag : 1;
            uint32_t fence : 1;
            uint32_t signaled : 1;
            uint32_t reserved : 28;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfs_wr_flag_t urma_jfs_wr_flag_t;

struct urma_jfs_wr;
typedef struct urma_jfs_wr urma_jfs_wr_t;

struct urma_jfs_wr {
    urma_opcode_t opcode;
    urma_jfs_wr_flag_t flag;
    urma_target_jetty_t *tjetty;
    uint64_t user_ctx;
    urma_rw_wr_t rw;
    urma_jfs_wr_t *next;
};

typedef struct {
    uint32_t value;
} urma_jfs_wr_flag;

struct urma_jfr_wr {
    urma_sg_t src;
    uint64_t user_ctx;
    struct urma_jfr_wr *next;
};
typedef struct urma_jfr_wr urma_jfr_wr_t;

struct urma_cr_t {
    urma_cr_status_t status;
    union {
        uint32_t completion_len;
        uint32_t byte_cnt;
    };
    uint32_t local_id;
    uint64_t user_ctx;
    int opcode;
    int immediate_data;
    uint64_t imm_data;
    int reserved;
};
typedef struct urma_cr_t urma_cr_t;

struct urma_seg {
    struct urma_ubva {
        urma_eid_t eid;
        uint32_t uasid;
        uint64_t va;
    } ubva;
    size_t len;
    struct {
        union {
            struct {
                uint32_t local_write : 1;
                uint32_t remote_write : 1;
                uint32_t remote_read : 1;
                uint32_t remote_atomic : 1;
                uint32_t reserved : 28;
            };
            uint32_t value;
        };
    } attr;
    uint32_t token_id;
    void *priv;
};
typedef struct urma_seg urma_seg_t;

struct urma_target_seg {
    urma_seg_t seg;
    struct {
        uint32_t token_id;
        uint32_t reserved;
    } *token_id;
    void *priv;
};

struct urma_token_id {
    uint32_t token_id;
    uint32_t reserved;
};
typedef struct urma_token_id urma_token_id_t;

struct urma_token {
    union {
        uint64_t token;
        uint64_t value;
    };
};
typedef struct urma_token urma_token_t;

struct urma_reg_seg_flag {
    uint32_t value;
    struct {
        uint32_t access;
        uint32_t token_policy;
        uint32_t token_id_valid;
        uint32_t cacheable;
        uint32_t reserved;
    } bs;
};
typedef struct urma_reg_seg_flag urma_reg_seg_flag_t;

struct urma_import_seg_flag {
    uint32_t value;
    struct {
        uint32_t access;
        uint32_t cacheable;
        uint32_t mapping;
        uint32_t reserved;
    } bs;
};
typedef struct urma_import_seg_flag urma_import_seg_flag_t;

struct urma_seg_cfg {
    uint64_t va;
    uint64_t iova;
    size_t len;
    int access;
    urma_reg_seg_flag_t flag;
    int pgsize_shift;
    void *cookie;
    void *user_ctx;
    urma_token_t token_value;
    urma_token_id_t *token_id;
};
typedef struct urma_seg_cfg urma_seg_cfg_t;

struct urma_jfc_flag {
    union {
        struct {
            uint32_t reserved0 : 1;
            uint32_t reserved1 : 31;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfc_flag urma_jfc_flag_t;

struct urma_jfc_cfg {
    int depth;
    int max_sge;
    urma_jfc_flag_t flag;
    void *jfce;
    void *user_ctx;
    int ceqn;
};
typedef struct urma_jfc_cfg urma_jfc_cfg_t;

struct urma_jfr_flag {
    union {
        struct {
            uint32_t tag_matching : 1;
            uint32_t reserved : 31;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfr_flag urma_jfr_flag_t;

struct urma_jfr_cfg {
    int depth;
    int max_sge;
    int max_wr;
    urma_jfr_flag_t flag;
    int trans_mode;
    int min_rnr_timer;
    int rnr_retry;
    void *jfc;
    urma_token_t token_value;
    uint32_t id;
    void *user_ctx;
};
typedef struct urma_jfr_cfg urma_jfr_cfg_t;

struct urma_jfs_flag {
    union {
        struct {
            uint32_t multi_path : 1;
            uint32_t reserved : 31;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfs_flag urma_jfs_flag_t;

struct urma_jfs_cfg {
    int depth;
    int priority;
    int trans_mode;
    int rnr_retry;
    int min_rnr_timer;
    int max_send_wr;
    int max_send_sge;
    int max_inline_data;
    int max_sge;
    int err_timeout;
    void *jfc;
    urma_jfs_flag_t flag;
    void *user_ctx;
};
typedef struct urma_jfs_cfg urma_jfs_cfg_t;

struct urma_jetty_flag {
    union {
        struct {
            uint32_t share_jfr : 1;
            uint32_t has_drv_ext : 1;
            uint32_t reserved : 30;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jetty_flag urma_jetty_flag_t;

struct urma_jetty_shared {
    void *jfr;
    void *jfc;
};
typedef struct urma_jetty_shared urma_jetty_shared_t;

struct urma_jetty_cfg {
    int access;
    int pgsize_shift;
    int max_send_wr;
    int max_recv_wr;
    int max_send_sge;
    int max_recv_sge;
    int max_inline_data;
    urma_jetty_flag_t flag;
    urma_jfs_cfg_t jfs_cfg;
    urma_jetty_shared_t shared;
    void *user_ctx;
};
typedef struct urma_jetty_cfg urma_jetty_cfg_t;

struct urma_jetty_attr {
    uint32_t mask;
    uint32_t state;
};
typedef struct urma_jetty_attr urma_jetty_attr_t;

struct urma_jfs_attr {
    uint32_t mask;
    uint32_t state;
};
typedef struct urma_jfs_attr urma_jfs_attr_t;

struct urma_tp_type_en {
    union {
        struct {
            uint32_t ctp : 1;
            uint32_t rtp : 1;
            uint32_t reserved : 30;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_tp_type_en urma_tp_type_en_t;

struct urma_priority_info {
    urma_tp_type_en_t tp_type;
    uint32_t SL;
    uint32_t reserved;
};
typedef struct urma_priority_info urma_priority_info_t;

#ifndef URMA_MAX_PRIORITY
#define URMA_MAX_PRIORITY 7
#endif

struct urma_dev_cap {
    uint64_t max_write_size;
    uint64_t max_read_size;
    uint64_t max_send_wr;
    uint64_t max_recv_wr;
    int max_jetty;
    int max_jfc;
    int max_jfc_depth;
    int max_jfr;
    int page_size;
    urma_priority_info_t priority_info[URMA_MAX_PRIORITY + 1];
};
typedef struct urma_dev_cap urma_dev_cap_t;

struct urma_device_attr {
    int dev_type;
    int eid_cnt;
    urma_dev_cap_t dev_cap;
    int max_jetty;
    int max_jfc;
    int max_jfr;
    int max_send_wr;
    int max_recv_wr;
    int max_send_sge;
    int max_recv_sge;
    int max_inline_data;
    int max_seg_cnt;
    int page_size;
};
typedef struct urma_device_attr urma_device_attr_t;

struct urma_eid_info {
    urma_eid_t eid;
    int eid_index;
    uint8_t reserved[URMA_EID_INFO_RESERVED_SIZE];
};
typedef struct urma_eid_info urma_eid_info_t;

struct urma_rjetty {
    urma_obj_id_t jetty_id;
    int trans_mode;
    int type;
    urma_tp_type_en_t tp_type;
    urma_jetty_flag_t flag;
    uint8_t data[URMA_RJETTY_DATA_SIZE];
};
typedef struct urma_rjetty urma_rjetty_t;

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
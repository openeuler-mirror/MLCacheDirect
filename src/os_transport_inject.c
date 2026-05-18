/**
 * @file os_transport_inject.c
 * @brief 故障注入实现（跨进程共享内存版 - 线程安全修复版）
 */

#include "os_transport_inject.h"

#if defined(OS_TRANSPORT_WITH_INJECT) && OS_TRANSPORT_WITH_INJECT

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <sched.h>
#include <pthread.h>

/* 进程本地缓存 */
static os_transport_inject_registry_t *g_shm = NULL;
static int g_shm_fd = -1;
static pthread_mutex_t g_init_mutex = PTHREAD_MUTEX_INITIALIZER;

/* 改进的自旋锁 - 带内存屏障 */
static inline void spin_lock(volatile int *lock)
{
    while (__sync_lock_test_and_set(lock, 1)) {
        while (*lock) {
            sched_yield();
        }
    }
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
}

static inline void spin_unlock(volatile int *lock)
{
    __atomic_thread_fence(__ATOMIC_RELEASE);
    __sync_lock_release(lock);
}

/* 创建/附加共享内存 - 线程安全修复版 */
os_transport_inject_registry_t *os_transport_inject_init(void)
{
    os_transport_inject_registry_t *shm = __atomic_load_n(&g_shm, __ATOMIC_ACQUIRE);
    if (shm != NULL) {
        return shm;
    }

    pthread_mutex_lock(&g_init_mutex);

    shm = __atomic_load_n(&g_shm, __ATOMIC_RELAXED);
    if (shm != NULL) {
        pthread_mutex_unlock(&g_init_mutex);
        return shm;
    }

    int fd = -1;
    int created = 0;

    fd = shm_open(OS_TRANSPORT_INJECT_SHM_NAME, O_RDWR, 0666);

    if (fd < 0 && errno == ENOENT) {
        fd = shm_open(OS_TRANSPORT_INJECT_SHM_NAME, O_CREAT | O_EXCL | O_RDWR, 0666);

        if (fd < 0) {
            if (errno == EEXIST) {
                fd = shm_open(OS_TRANSPORT_INJECT_SHM_NAME, O_RDWR, 0666);
                if (fd < 0) {
                    perror("[OS_TRANSPORT_INJECT] shm_open failed");
                    pthread_mutex_unlock(&g_init_mutex);
                    return NULL;
                }
            } else {
                perror("[OS_TRANSPORT_INJECT] shm_open failed");
                pthread_mutex_unlock(&g_init_mutex);
                return NULL;
            }
        } else {
            created = 1;
            if (ftruncate(fd, sizeof(os_transport_inject_registry_t)) < 0) {
                perror("[OS_TRANSPORT_INJECT] ftruncate failed");
                close(fd);
                shm_unlink(OS_TRANSPORT_INJECT_SHM_NAME);
                pthread_mutex_unlock(&g_init_mutex);
                return NULL;
            }
        }
    } else if (fd < 0) {
        perror("[OS_TRANSPORT_INJECT] shm_open failed");
        pthread_mutex_unlock(&g_init_mutex);
        return NULL;
    }

    void *addr = mmap(NULL, sizeof(os_transport_inject_registry_t), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) {
        perror("[OS_TRANSPORT_INJECT] mmap failed");
        close(fd);
        if (created) {
            shm_unlink(OS_TRANSPORT_INJECT_SHM_NAME);
        }
        pthread_mutex_unlock(&g_init_mutex);
        return NULL;
    }

    g_shm_fd = fd;
    shm = (os_transport_inject_registry_t *)addr;

    if (created) {
        memset(shm, 0, sizeof(os_transport_inject_registry_t));
        __atomic_store_n(&shm->header.version, OS_TRANSPORT_INJECT_VERSION, __ATOMIC_RELEASE);
        __atomic_store_n(&shm->header.seq, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&shm->header.lock, 0, __ATOMIC_RELEASE);
        __atomic_store_n(&shm->header.count, 0, __ATOMIC_RELEASE);
        fprintf(stderr, "[OS_TRANSPORT_INJECT] Shared memory created by pid=%d\n", getpid());
    } else {
        int spin = 0;
        uint32_t version = 0;
        while (spin < 1000) {
            version = __atomic_load_n(&shm->header.version, __ATOMIC_ACQUIRE);
            if (version != 0)
                break;
            usleep(1000);
            spin++;
        }

        if (version == 0) {
            fprintf(stderr, "[OS_TRANSPORT_INJECT] Timeout waiting for init\n");
            munmap(shm, sizeof(os_transport_inject_registry_t));
            close(fd);
            pthread_mutex_unlock(&g_init_mutex);
            return NULL;
        }

        if (version != OS_TRANSPORT_INJECT_VERSION) {
            fprintf(stderr, "[OS_TRANSPORT_INJECT] Version mismatch, resetting\n");
            spin_lock(&shm->header.lock);
            memset(shm->points, 0, sizeof(shm->points));
            memset(shm->ring, 0, sizeof(shm->ring));
            __atomic_store_n(&shm->ring_tail, 0, __ATOMIC_RELEASE);
            __atomic_store_n(&shm->header.version, OS_TRANSPORT_INJECT_VERSION, __ATOMIC_RELEASE);
            __atomic_store_n(&shm->header.seq, 1, __ATOMIC_RELEASE);
            __atomic_store_n(&shm->header.count, 0, __ATOMIC_RELEASE);
            __atomic_store_n(&shm->header.writer_pid, getpid(), __ATOMIC_RELEASE);
            spin_unlock(&shm->header.lock);
        } else {
            uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_ACQUIRE);
            fprintf(stderr, "[OS_TRANSPORT_INJECT] Attached (points=%u)\n", count);
        }
    }

    __atomic_store_n(&g_shm, shm, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&g_init_mutex);
    return shm;
}

int os_transport_inject_set_sleep(const char *name, uint32_t ms)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return -1;

    spin_lock(&shm->header.lock);

    os_transport_inject_point_t *p = NULL;
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(shm->points[i].name, name) == 0) {
            p = &shm->points[i];
            break;
        }
    }

    if (!p && count < OS_TRANSPORT_MAX_INJECT_POINTS) {
        p = &shm->points[count];
        __atomic_fetch_add(&shm->header.count, 1, __ATOMIC_ACQ_REL);
        memset(p, 0, sizeof(os_transport_inject_point_t));
        strncpy(p->name, name, OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1);
        p->name[OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1] = '\0';
    }

    int ret = -1;
    if (p) {
        p->action_type = OS_TRANSPORT_INJECT_ACTION_SLEEP;
        p->sleep_ms = ms;
        __atomic_store_n(&p->enabled, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&shm->header.writer_pid, getpid(), __ATOMIC_RELEASE);
        __atomic_fetch_add(&shm->header.seq, 1, __ATOMIC_ACQ_REL);
        ret = 0;
    }

    spin_unlock(&shm->header.lock);

    if (ret == 0) {
        fprintf(stderr, "[OS_TRANSPORT_INJECT] Set: %s = sleep(%u ms)\n", name, ms);
    }
    return ret;
}

int os_transport_inject_set_return_error(const char *name, int error_code)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return -1;

    spin_lock(&shm->header.lock);

    os_transport_inject_point_t *p = NULL;
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(shm->points[i].name, name) == 0) {
            p = &shm->points[i];
            break;
        }
    }

    if (!p && count < OS_TRANSPORT_MAX_INJECT_POINTS) {
        p = &shm->points[count];
        __atomic_fetch_add(&shm->header.count, 1, __ATOMIC_ACQ_REL);
        memset(p, 0, sizeof(os_transport_inject_point_t));
        strncpy(p->name, name, OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1);
        p->name[OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1] = '\0';
    }

    int ret = -1;
    if (p) {
        p->action_type = OS_TRANSPORT_INJECT_ACTION_RETURN_ERROR;
        p->return_value = error_code;
        __atomic_store_n(&p->enabled, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&shm->header.writer_pid, getpid(), __ATOMIC_RELEASE);
        __atomic_fetch_add(&shm->header.seq, 1, __ATOMIC_ACQ_REL);
        ret = 0;
    }

    spin_unlock(&shm->header.lock);

    if (ret == 0) {
        fprintf(stderr, "[OS_TRANSPORT_INJECT] Set: %s = error(%d)\n", name, error_code);
    }
    return ret;
}

int os_transport_inject_set_print(const char *name, const char *message)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return -1;

    spin_lock(&shm->header.lock);

    os_transport_inject_point_t *p = NULL;
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(shm->points[i].name, name) == 0) {
            p = &shm->points[i];
            break;
        }
    }

    if (!p && count < OS_TRANSPORT_MAX_INJECT_POINTS) {
        p = &shm->points[count];
        __atomic_fetch_add(&shm->header.count, 1, __ATOMIC_ACQ_REL);
        memset(p, 0, sizeof(os_transport_inject_point_t));
        strncpy(p->name, name, OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1);
        p->name[OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1] = '\0';
    }

    int ret = -1;
    if (p) {
        p->action_type = OS_TRANSPORT_INJECT_ACTION_PRINT;
        strncpy(p->message, message, OS_TRANSPORT_INJECT_ACTION_MAX_LEN - 1);
        p->message[OS_TRANSPORT_INJECT_ACTION_MAX_LEN - 1] = '\0';
        __atomic_store_n(&p->enabled, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&shm->header.writer_pid, getpid(), __ATOMIC_RELEASE);
        __atomic_fetch_add(&shm->header.seq, 1, __ATOMIC_ACQ_REL);
        ret = 0;
    }

    spin_unlock(&shm->header.lock);
    return ret;
}

int os_transport_inject_set_frequency(const char *name, uint32_t percent_or_count)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return -1;

    spin_lock(&shm->header.lock);

    os_transport_inject_point_t *p = NULL;
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(shm->points[i].name, name) == 0) {
            p = &shm->points[i];
            break;
        }
    }

    int ret = -1;
    if (p) {
        if (percent_or_count <= 100) {
            p->frequency_percent = percent_or_count;
            p->trigger_limit = 0;
        } else {
            uint32_t cnt = percent_or_count - 100;
            __atomic_store_n(&p->trigger_count, cnt, __ATOMIC_RELEASE);
            p->trigger_limit = cnt;
            p->frequency_percent = 0;
        }
        __atomic_fetch_add(&shm->header.seq, 1, __ATOMIC_ACQ_REL);
        ret = 0;
    }

    spin_unlock(&shm->header.lock);
    return ret;
}

void os_transport_inject_clear(const char *name)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return;

    spin_lock(&shm->header.lock);
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(shm->points[i].name, name) == 0) {
            __atomic_store_n(&shm->points[i].enabled, 0, __ATOMIC_RELEASE);
            fprintf(stderr, "[OS_TRANSPORT_INJECT] Cleared: %s\n", name);
            break;
        }
    }
    __atomic_fetch_add(&shm->header.seq, 1, __ATOMIC_ACQ_REL);
    spin_unlock(&shm->header.lock);
}

void os_transport_inject_clear_all(void)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return;

    spin_lock(&shm->header.lock);
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        __atomic_store_n(&shm->points[i].enabled, 0, __ATOMIC_RELEASE);
        uint32_t limit = shm->points[i].trigger_limit;
        __atomic_store_n(&shm->points[i].trigger_count, limit, __ATOMIC_RELEASE);
    }
    __atomic_fetch_add(&shm->header.seq, 1, __ATOMIC_ACQ_REL);
    spin_unlock(&shm->header.lock);

    fprintf(stderr, "[OS_TRANSPORT_INJECT] All cleared\n");
}

int os_transport_inject_execute(const char *name, int *out_return_value)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return 0;

    os_transport_inject_point_t *p = NULL;
    int should_trigger = 0;
    os_transport_inject_action_type_t action = OS_TRANSPORT_INJECT_ACTION_NONE;
    int return_val = 0;
    uint32_t sleep_ms = 0;
    char message[OS_TRANSPORT_INJECT_ACTION_MAX_LEN];

    spin_lock(&shm->header.lock);
    uint32_t count = __atomic_load_n(&shm->header.count, __ATOMIC_RELAXED);

    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(shm->points[i].name, name) == 0) {
            p = &shm->points[i];
            break;
        }
    }

    if (p) {
        int enabled = __atomic_load_n(&p->enabled, __ATOMIC_ACQUIRE);
        if (enabled) {
            uint32_t trigger_limit = p->trigger_limit;
            if (trigger_limit > 0) {
                uint32_t old_val = __atomic_fetch_sub(&p->trigger_count, 1, __ATOMIC_ACQ_REL);
                if (old_val > 0) {
                    should_trigger = 1;
                } else {
                    __atomic_fetch_add(&p->trigger_count, 1, __ATOMIC_ACQ_REL);
                }
            } else if (p->frequency_percent > 0 && p->frequency_percent < 100) {
                static __thread unsigned int seed = 0;
                if (seed == 0)
                    seed = (unsigned int)time(NULL) ^ (unsigned int)getpid();
                should_trigger = (uint32_t)(rand_r(&seed) % 100) < p->frequency_percent;
            } else {
                should_trigger = 1;
            }

            if (should_trigger) {
                action = p->action_type;
                return_val = p->return_value;
                sleep_ms = p->sleep_ms;
                memcpy(message, p->message, OS_TRANSPORT_INJECT_ACTION_MAX_LEN);

                /* Write trigger record to Ring Buffer */
                uint32_t tail = __atomic_load_n(&shm->ring_tail, __ATOMIC_RELAXED);
                uint32_t next = (tail + 1) % OS_TRANSPORT_INJECT_RING_SIZE;
                struct timespec ts;
                clock_gettime(CLOCK_MONOTONIC, &ts);
                shm->ring[tail].timestamp_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
                strncpy(shm->ring[tail].name, name, OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1);
                shm->ring[tail].name[OS_TRANSPORT_INJECT_NAME_MAX_LEN - 1] = '\0';
                shm->ring[tail].action_type = action;
                shm->ring[tail].return_value = return_val;
                __atomic_store_n(&shm->ring_tail, next, __ATOMIC_RELEASE);
            }
        }
    }
    spin_unlock(&shm->header.lock);

    if (!should_trigger) {
        return 0;
    }

    switch (action) {
    case OS_TRANSPORT_INJECT_ACTION_SLEEP:
        fprintf(stderr, "[OS_TRANSPORT_INJECT] Executing sleep: %s = %u ms\n", name, sleep_ms);
        usleep(sleep_ms * 1000);
        return 0;

    case OS_TRANSPORT_INJECT_ACTION_RETURN_ERROR:
        fprintf(stderr, "[OS_TRANSPORT_INJECT] Executing error: %s = %d\n", name, return_val);
        if (out_return_value)
            *out_return_value = return_val;
        return 1;

    case OS_TRANSPORT_INJECT_ACTION_PRINT:
        fprintf(stderr, "[OS_TRANSPORT_INJECT] %s: %s\n", name, message);
        return 0;

    default:
        return 0;
    }
}

uint32_t os_transport_inject_get_count(void)
{
    os_transport_inject_registry_t *shm = os_transport_inject_init();
    if (!shm)
        return 0;
    return __atomic_load_n(&shm->header.count, __ATOMIC_ACQUIRE);
}

void os_transport_inject_cleanup(void)
{
    fprintf(stderr, "[OS_TRANSPORT_INJECT] Cleaning up shared memory...\n");

    if (g_shm) {
        munmap(g_shm, sizeof(os_transport_inject_registry_t));
        g_shm = NULL;
    }
    if (g_shm_fd >= 0) {
        close(g_shm_fd);
        g_shm_fd = -1;
    }
    shm_unlink(OS_TRANSPORT_INJECT_SHM_NAME);
}

#endif /* OS_TRANSPORT_WITH_INJECT */

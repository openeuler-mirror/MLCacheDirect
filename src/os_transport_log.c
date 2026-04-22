#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#include "os_transport_log_internal.h"
#include "os_transport.h"

#define OST_LOG_FILE_ENV "OST_LOG_FILE_PATH"

// 全局日志级别控制（可通过编译宏/配置修改）
#ifndef GLOBAL_LOG_LEVEL
#define GLOBAL_LOG_LEVEL LOG_LEVEL_DEBUG
#endif

typedef enum { OST_LOG_BACKEND_UNINITIALIZED = 0, OST_LOG_BACKEND_FILE, OST_LOG_BACKEND_DISABLED } OstLogBackend;

typedef struct {
    pthread_mutex_t mutex;
    bool initialized;
    int fd;
    log_callback_t callback;
    int callback_level;
    OstLogBackend backend;
} OstLogState;

static inline OstLogState *ost_log_state(void)
{
    static OstLogState state = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .initialized = false,
        .fd = -1,
        .callback = NULL,
        .callback_level = LOG_LEVEL_INFO,
        .backend = OST_LOG_BACKEND_UNINITIALIZED,
    };

    return &state;
}

static inline const char *ost_log_level_name(LogLevel level)
{
    switch (level) {
    case LOG_LEVEL_DEBUG:
        return "DEBUG";
    case LOG_LEVEL_INFO:
        return "INFO";
    case LOG_LEVEL_WARN:
        return "WARN";
    case LOG_LEVEL_ERROR:
        return "ERROR";
    default:
        return "INFO";
    }
}

static inline long ost_log_get_tid(void)
{
#ifdef SYS_gettid
    return syscall(SYS_gettid);
#else
    return (long)getpid();
#endif
}

static inline int ost_log_open_file_fd(const char *path)
{
    int fd;

    if (path == NULL || path[0] == '\0') {
        return -1;
    }

    fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd >= 0) {
        (void)fcntl(fd, F_SETFD, FD_CLOEXEC);
    }
    return fd;
}

static inline void ost_log_close_locked(OstLogState *state)
{
    if (state->fd >= 0) {
        close(state->fd);
        state->fd = -1;
    }

    state->backend = OST_LOG_BACKEND_UNINITIALIZED;
    state->initialized = false;
}

static void ost_log_init_locked(OstLogState *state)
{
    const char *file_path = getenv(OST_LOG_FILE_ENV);

    state->backend = OST_LOG_BACKEND_DISABLED;
    state->fd = -1;

    if (file_path != NULL && file_path[0] != '\0') {
        state->fd = ost_log_open_file_fd(file_path);
        if (state->fd >= 0) {
            state->backend = OST_LOG_BACKEND_FILE;
            state->initialized = true;
            return;
        }
    }

    state->backend = OST_LOG_BACKEND_DISABLED;
    state->initialized = true;
}

static int ost_log_normalize_callback_level(int level)
{
    if (level <= LOG_LEVEL_INFO) {
        return LOG_LEVEL_INFO;
    }

    return level;
}

int os_transport_log_reg(int level, log_callback_t cb)
{
    OstLogState *state = ost_log_state();

    pthread_mutex_lock(&state->mutex);
    state->callback = cb;
    state->callback_level = ost_log_normalize_callback_level(level);
    pthread_mutex_unlock(&state->mutex);
    return 0;
}

static size_t ost_log_format_callback_line(char *buf, size_t buf_size, const char *file, int line, const char *message)
{
    int written;

    if (buf_size == 0) {
        return 0;
    }

    written = snprintf(buf, buf_size, "%s:%d %s\n", file != NULL ? file : "<unknown>", line, message);
    if (written < 0) {
        buf[0] = '\0';
        return 0;
    }

    if ((size_t)written >= buf_size) {
        buf[buf_size - 1] = '\0';
        return buf_size - 1;
    }

    return (size_t)written;
}

static size_t
ost_log_format_line(char *buf, size_t buf_size, LogLevel level, const char *file, int line, const char *message)
{
    struct timespec ts;
    struct tm tm_info;
    char time_buf[32];
    int prefix_len;
    int body_len;

    (void)clock_gettime(CLOCK_REALTIME, &ts);
    (void)localtime_r(&ts.tv_sec, &tm_info);
    (void)strftime(time_buf, sizeof(time_buf), "%Y-%m-%dT%H:%M:%S", &tm_info);

    prefix_len = snprintf(buf,
                          buf_size,
                          "%s.%03ld [%s] [pid=%ld tid=%ld] [ost:%s:%d] ",
                          time_buf,
                          ts.tv_nsec / 1000000L,
                          ost_log_level_name(level),
                          (long)getpid(),
                          ost_log_get_tid(),
                          file,
                          line);
    if (prefix_len < 0) {
        return 0;
    }

    if ((size_t)prefix_len >= buf_size) {
        buf[buf_size - 1] = '\0';
        return buf_size - 1;
    }

    body_len = snprintf(buf + prefix_len, buf_size - (size_t)prefix_len, "%s\n", message);
    if (body_len < 0) {
        return (size_t)prefix_len;
    }

    if ((size_t)(prefix_len + body_len) >= buf_size) {
        buf[buf_size - 1] = '\0';
        return buf_size - 1;
    }

    return (size_t)(prefix_len + body_len);
}

static inline void ost_log_write_fd_best_effort(int fd, const char *buf, size_t len)
{
    size_t total_written = 0;

    while (total_written < len) {
        ssize_t written = write(fd, buf + total_written, len - total_written);
        if (written > 0) {
            total_written += (size_t)written;
            continue;
        }

        if (written < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

static void ost_log_emit_locked(OstLogState *state, LogLevel level, const char *formatted_line)
{
    size_t len = strlen(formatted_line);

    (void)level;

    switch (state->backend) {
    case OST_LOG_BACKEND_FILE:
        if (state->fd >= 0) {
            ost_log_write_fd_best_effort(state->fd, formatted_line, len);
        }
        break;
    case OST_LOG_BACKEND_DISABLED:
    case OST_LOG_BACKEND_UNINITIALIZED:
    default:
        break;
    }
}

static void ost_log_vwrite(LogLevel level, const char *file, int line, const char *fmt, va_list args)
{
    char msg_buf[1024];
    char line_buf[1408];
    OstLogState *state = ost_log_state();
    log_callback_t callback = NULL;

    pthread_mutex_lock(&state->mutex);
    if (state->callback != NULL) {
        if (level != LOG_LEVEL_DEBUG && level >= state->callback_level) {
            callback = state->callback;
        }
        pthread_mutex_unlock(&state->mutex);
        if (callback == NULL) {
            return;
        }

        (void)vsnprintf(msg_buf, sizeof(msg_buf), fmt, args);
        (void)ost_log_format_callback_line(line_buf, sizeof(line_buf), file, line, msg_buf);
        callback(level, line_buf);
        return;
    }

    if (!state->initialized) {
        ost_log_init_locked(state);
    }
    if (state->backend != OST_LOG_BACKEND_FILE || state->fd < 0 || level < GLOBAL_LOG_LEVEL) {
        pthread_mutex_unlock(&state->mutex);
        return;
    }

    (void)vsnprintf(msg_buf, sizeof(msg_buf), fmt, args);
    (void)ost_log_format_line(line_buf, sizeof(line_buf), level, file, line, msg_buf);
    ost_log_emit_locked(state, level, line_buf);
    pthread_mutex_unlock(&state->mutex);
}

void ost_log_write(LogLevel level, const char *file, int line, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    ost_log_vwrite(level, file, line, fmt, args);
    va_end(args);
}

#ifdef OST_LOG_ENABLE_TEST_HOOKS
void ost_log_reset_for_tests(void)
{
    OstLogState *state = ost_log_state();

    pthread_mutex_lock(&state->mutex);
    ost_log_close_locked(state);
    state->callback = NULL;
    state->callback_level = LOG_LEVEL_INFO;
    pthread_mutex_unlock(&state->mutex);
}

void ost_log_force_syslog_available_for_tests(bool available)
{
    (void)available;
}
#endif

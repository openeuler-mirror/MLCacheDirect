#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/types.h>
#include <syslog.h>
#include <time.h>
#include <unistd.h>

#include "os_transport_log_internal.h"
#include "os_transport.h"

#define OST_LOG_FILE_ENV "OST_LOG_FILE_PATH"

// 全局日志级别控制（可通过编译宏/配置修改）
#ifndef GLOBAL_LOG_LEVEL
#define GLOBAL_LOG_LEVEL LOG_LEVEL_DEBUG
#endif

#ifndef likely
#define likely(x) __builtin_expect(!!(x), 1)
#endif

typedef enum {
    OST_LOG_BACKEND_UNINITIALIZED = 0,
    OST_LOG_BACKEND_FILE,
    OST_LOG_BACKEND_SYSLOG,
    OST_LOG_BACKEND_CALLBACK,
    OST_LOG_BACKEND_DISABLED
} OstLogBackend;

typedef struct {
    pthread_mutex_t mutex;
    bool syslog_opened;
    int fd;
    log_callback_t callback;
    LogLevel log_level;
    OstLogBackend backend;
} OstLogState;

typedef struct {
    OstLogBackend backend;
    int fd;
    log_callback_t callback;
} OstLogEmitTarget;

static inline OstLogState *ost_log_state(void)
{
    static OstLogState state = {
        .mutex = PTHREAD_MUTEX_INITIALIZER,
        .syslog_opened = false,
        .fd = -1,
        .callback = NULL,
        .log_level = GLOBAL_LOG_LEVEL,
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

static inline int ost_log_syslog_priority(LogLevel level)
{
    switch (level) {
    case LOG_LEVEL_DEBUG:
        return LOG_DEBUG;
    case LOG_LEVEL_INFO:
        return LOG_INFO;
    case LOG_LEVEL_WARN:
        return LOG_WARNING;
    case LOG_LEVEL_ERROR:
        return LOG_ERR;
    default:
        return LOG_INFO;
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

static inline bool ost_log_path_exists(const char *path)
{
    struct stat st;

    if (path == NULL || path[0] == '\0') {
        return false;
    }

    return stat(path, &st) == 0;
}

static inline void ost_log_close_locked(OstLogState *state)
{
    if (state->fd >= 0) {
        close(state->fd);
        state->fd = -1;
    }

    if (state->syslog_opened) {
        closelog();
        state->syslog_opened = false;
    }

    state->backend = OST_LOG_BACKEND_UNINITIALIZED;
}

#ifdef OST_LOG_ENABLE_TEST_HOOKS
static inline int *ost_log_syslog_probe_override(void)
{
    static int override_available = -1;

    return &override_available;
}
#endif

static inline bool ost_log_syslog_available_probe(void)
{
#ifdef OST_LOG_ENABLE_TEST_HOOKS
    if (*ost_log_syslog_probe_override() != -1) {
        return *ost_log_syslog_probe_override() == 1;
    }
#endif

    return ost_log_path_exists("/dev/log") || ost_log_path_exists("/run/systemd/journal/dev-log")
           || ost_log_path_exists("/run/systemd/journal/syslog");
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

static bool ost_log_install_file_backend_locked(OstLogState *state)
{
    const char *file_path = getenv(OST_LOG_FILE_ENV);
    int fd;

    fd = ost_log_open_file_fd(file_path);
    if (fd < 0) {
        return false;
    }

    if (state->syslog_opened) {
        closelog();
        state->syslog_opened = false;
    }
    if (state->fd >= 0) {
        close(state->fd);
    }
    state->fd = fd;
    state->backend = OST_LOG_BACKEND_FILE;
    return true;
}

static void ost_log_select_backend_locked(OstLogState *state)
{
    /*
     * Backend priority is monotonic upward:
     * callback > file > syslog > disabled/uninitialized.
     */
    if (state->backend == OST_LOG_BACKEND_CALLBACK || state->backend == OST_LOG_BACKEND_FILE) {
        return;
    }

    if (ost_log_install_file_backend_locked(state)) {
        return;
    }

    if (state->backend == OST_LOG_BACKEND_SYSLOG) {
        return;
    }

    if (ost_log_syslog_available_probe()) {
        openlog("os_transport", LOG_PID | LOG_NDELAY, LOG_USER);
        state->syslog_opened = true;
        state->backend = OST_LOG_BACKEND_SYSLOG;
        return;
    }

    state->backend = OST_LOG_BACKEND_DISABLED;
}

int os_transport_log_reg(int level, log_callback_t cb)
{
    if (!cb) {
        return -1;
    }

    OstLogState *state = ost_log_state();

    pthread_mutex_lock(&state->mutex);
    ost_log_close_locked(state);
    state->callback = cb;
    state->log_level = (LogLevel)level;
    state->backend = OST_LOG_BACKEND_CALLBACK;
    pthread_mutex_unlock(&state->mutex);
    return 0;
}

static size_t
ost_log_format_line(char *buf,
                    size_t buf_size,
                    OstLogBackend backend,
                    LogLevel level,
                    const char *file,
                    int line,
                    const char *message)
{
    int prefix_len;
    int body_len;

    if (likely(backend != OST_LOG_BACKEND_FILE)) {
        prefix_len = snprintf(buf, buf_size, "[ost:%s:%d] ", file, line);
    } else {
        struct timespec ts;
        struct tm tm_info;
        char time_buf[32];

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
    }
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

static void ost_log_emit(OstLogEmitTarget target, LogLevel level, const char *formatted_line)
{
    size_t len = strlen(formatted_line);

    switch (target.backend) {
    case OST_LOG_BACKEND_FILE:
        if (target.fd >= 0) {
            ost_log_write_fd_best_effort(target.fd, formatted_line, len);
        }
        break;
    case OST_LOG_BACKEND_SYSLOG:
        syslog(ost_log_syslog_priority(level), "%s", formatted_line);
        break;
    case OST_LOG_BACKEND_CALLBACK:
        if (target.callback) {
            target.callback(level, formatted_line);
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
    OstLogEmitTarget target = {.backend = OST_LOG_BACKEND_DISABLED, .fd = -1, .callback = NULL};

    pthread_mutex_lock(&state->mutex);
    ost_log_select_backend_locked(state);
    if (level < state->log_level) {
        pthread_mutex_unlock(&state->mutex);
        return;
    }
    target.backend = state->backend;
    if (state->backend == OST_LOG_BACKEND_FILE && state->fd >= 0) {
        target.fd = dup(state->fd);
    } else if (state->backend == OST_LOG_BACKEND_CALLBACK) {
        target.callback = state->callback;
    }
    pthread_mutex_unlock(&state->mutex);

    (void)vsnprintf(msg_buf, sizeof(msg_buf), fmt, args);
    (void)ost_log_format_line(line_buf, sizeof(line_buf), target.backend, level, file, line, msg_buf);

    ost_log_emit(target, level, line_buf);
    if (target.fd >= 0) {
        close(target.fd);
    }
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
    *ost_log_syslog_probe_override() = -1;
    pthread_mutex_unlock(&state->mutex);
}

void ost_log_force_syslog_available_for_tests(bool available)
{
    *ost_log_syslog_probe_override() = available ? 1 : 0;
}
#endif

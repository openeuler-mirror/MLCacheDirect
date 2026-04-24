#ifndef OS_TRANSPORT_LOG_INTERNAL_H
#define OS_TRANSPORT_LOG_INTERNAL_H

#include <stdbool.h>
#include <stdint.h>

typedef enum { LOG_LEVEL_DEBUG = -1, LOG_LEVEL_INFO, LOG_LEVEL_WARN, LOG_LEVEL_ERROR } LogLevel;

void ost_log_write(LogLevel level, const char *file, int line, const char *fmt, ...);

#ifdef OST_LOG_ENABLE_TEST_HOOKS
void ost_log_reset_for_tests(void);
void ost_log_force_syslog_available_for_tests(bool available);
#endif

// 日志格式化输出宏
#define OST_LOG(level, fmt, ...) ost_log_write((level), __FILE__, __LINE__, (fmt), ##__VA_ARGS__)

// 快捷日志宏
#define OST_LOG_DEBUG(fmt, ...) OST_LOG(LOG_LEVEL_DEBUG, fmt, ##__VA_ARGS__)
#define OST_LOG_INFO(fmt, ...)  OST_LOG(LOG_LEVEL_INFO, fmt, ##__VA_ARGS__)
#define OST_LOG_WARN(fmt, ...)  OST_LOG(LOG_LEVEL_WARN, fmt, ##__VA_ARGS__)
#define OST_LOG_ERROR(fmt, ...) OST_LOG(LOG_LEVEL_ERROR, fmt, ##__VA_ARGS__)

#endif // OS_TRANSPORT_LOG_INTERNAL_H

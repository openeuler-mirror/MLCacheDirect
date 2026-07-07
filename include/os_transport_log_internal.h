#ifndef OS_TRANSPORT_LOG_INTERNAL_H
#define OS_TRANSPORT_LOG_INTERNAL_H

#include <stdbool.h>
#include <stdint.h>

typedef enum { LOG_LEVEL_DEBUG = -1, LOG_LEVEL_INFO, LOG_LEVEL_WARN, LOG_LEVEL_ERROR } LogLevel;

void ost_log_write(LogLevel level, int vlevel, const char *file, int line, const char *fmt, ...);

#ifdef OST_LOG_ENABLE_TEST_HOOKS
void ost_log_reset_for_tests(void);
void ost_log_force_syslog_available_for_tests(bool available);
#endif

#ifdef __FILE_NAME__
#define OST_LOG_FILE_NAME __FILE_NAME__
#else
#define OST_LOG_FILE_NAME __FILE__
#endif

// 日志格式化输出宏
#define OST_LOG(level, vlevel, fmt, ...) ost_log_write((level), (vlevel), OST_LOG_FILE_NAME, __LINE__, ("[PIPLN RH2D] "fmt), ##__VA_ARGS__)

// 快捷日志宏
#define OST_LOG_DEBUG(vlevel, fmt, ...) OST_LOG(LOG_LEVEL_DEBUG, vlevel, fmt, ##__VA_ARGS__)
#define OST_LOG_INFO(fmt, ...)  OST_LOG(LOG_LEVEL_INFO, -1, fmt, ##__VA_ARGS__)
#define OST_LOG_WARN(fmt, ...)  OST_LOG(LOG_LEVEL_WARN, -1, fmt, ##__VA_ARGS__)
#define OST_LOG_ERROR(fmt, ...) OST_LOG(LOG_LEVEL_ERROR, -1, fmt, ##__VA_ARGS__)

#endif // OS_TRANSPORT_LOG_INTERNAL_H

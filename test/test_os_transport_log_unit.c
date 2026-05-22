#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define OST_LOG_ENABLE_TEST_HOOKS 1
#include "../src/os_transport_log.c"

static int g_callback_calls = 0;
static int g_callback_level = 0;
static bool g_callback_saw_unlocked_mutex = false;
static char g_callback_msg[512];

static void reset_callback_state(void)
{
    g_callback_calls = 0;
    g_callback_level = 0;
    g_callback_saw_unlocked_mutex = false;
    memset(g_callback_msg, 0, sizeof(g_callback_msg));
}

static void test_log_callback(int level, const char *msg)
{
    g_callback_calls++;
    g_callback_level = level;
    if (msg) {
        (void)snprintf(g_callback_msg, sizeof(g_callback_msg), "%s", msg);
    }
}

static void test_lock_probe_callback(int level, const char *msg)
{
    (void)level;
    (void)msg;

    if (pthread_mutex_trylock(&ost_log_state()->mutex) == 0) {
        g_callback_saw_unlocked_mutex = true;
        pthread_mutex_unlock(&ost_log_state()->mutex);
    }
}

static void clear_log_env(void)
{
    unsetenv("OST_LOG_FILE_PATH");
}

static void build_tmp_log_path(char *path, size_t path_size, const char *name)
{
    (void)snprintf(path, path_size, "/tmp/os_transport_log_%s_%ld.log", name, (long)getpid());
    unlink(path);
}

static void assert_file_contains(const char *path, const char *expected)
{
    FILE *fp = NULL;
    char buf[512];

    fp = fopen(path, "r");
    assert(fp != NULL);
    assert(fgets(buf, sizeof(buf), fp) != NULL);
    assert(strstr(buf, expected) != NULL);
    fclose(fp);
}

static void test_log_format_uses_file_argument(void)
{
    char line[512];

    assert(ost_log_format_line(line,
                               sizeof(line),
                               OST_LOG_BACKEND_FILE,
                               LOG_LEVEL_ERROR,
                               "/workspace/project/src/os_transport.c",
                               88,
                               "file argument message")
           > 0);
    assert(strstr(line, "[ost:/workspace/project/src/os_transport.c:88]") != NULL);
}

static void test_file_backend_takes_priority_over_syslog(void)
{
    char path[128];

    clear_log_env();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(true);

    build_tmp_log_path(path, sizeof(path), "priority");
    assert(setenv("OST_LOG_FILE_PATH", path, 1) == 0);

    ost_log_write(LOG_LEVEL_WARN, "unit.c", 45, "file priority message");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_FILE);
    assert(ost_log_state()->fd >= 0);
    assert(ost_log_state()->syslog_opened == false);

    ost_log_reset_for_tests();
    assert_file_contains(path, "file priority message");
    assert_file_contains(path, "pid=");
    assert_file_contains(path, "tid=");
    unlink(path);
    clear_log_env();
}

static void test_callback_registration_overrides_existing_file_backend(void)
{
    char path[128];

    clear_log_env();
    reset_callback_state();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(false);

    build_tmp_log_path(path, sizeof(path), "callback");
    assert(setenv("OST_LOG_FILE_PATH", path, 1) == 0);

    ost_log_write(LOG_LEVEL_WARN, "unit.c", 56, "file before callback");
    assert(ost_log_state()->backend == OST_LOG_BACKEND_FILE);
    assert(ost_log_state()->fd >= 0);

    assert(os_transport_log_reg(LOG_LEVEL_DEBUG, test_log_callback) == 0);
    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 57, "callback after file");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_CALLBACK);
    assert(ost_log_state()->fd == -1);
    assert(g_callback_calls == 1);
    assert(strstr(g_callback_msg, "callback after file") != NULL);

    ost_log_reset_for_tests();
    unlink(path);
    clear_log_env();
}

static void test_callback_backend_omits_process_context(void)
{
    clear_log_env();
    reset_callback_state();
    ost_log_reset_for_tests();

    assert(os_transport_log_reg(LOG_LEVEL_DEBUG, test_log_callback) == 0);
    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 57, "callback compact message");

    assert(g_callback_calls == 1);
    assert(strcmp(g_callback_msg, "[ost:unit.c:57] callback compact message\n") == 0);
    assert(strstr(g_callback_msg, "pid=") == NULL);
    assert(strstr(g_callback_msg, "tid=") == NULL);
}

static void test_callback_registration_overrides_existing_syslog_backend(void)
{
    clear_log_env();
    reset_callback_state();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(true);

    ost_log_write(LOG_LEVEL_INFO, "unit.c", 68, "syslog before callback");
    assert(ost_log_state()->backend == OST_LOG_BACKEND_SYSLOG);
    assert(ost_log_state()->syslog_opened == true);

    assert(os_transport_log_reg(LOG_LEVEL_DEBUG, test_log_callback) == 0);
    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 69, "callback after syslog");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_CALLBACK);
    assert(ost_log_state()->syslog_opened == false);
    assert(g_callback_calls == 1);
    assert(strstr(g_callback_msg, "callback after syslog") != NULL);
}

static void test_file_backend_overrides_existing_syslog_backend(void)
{
    char path[128];

    clear_log_env();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(true);

    ost_log_write(LOG_LEVEL_INFO, "unit.c", 70, "syslog before file");
    assert(ost_log_state()->backend == OST_LOG_BACKEND_SYSLOG);
    assert(ost_log_state()->syslog_opened == true);

    build_tmp_log_path(path, sizeof(path), "syslog_to_file");
    assert(setenv("OST_LOG_FILE_PATH", path, 1) == 0);

    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 71, "file after syslog");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_FILE);
    assert(ost_log_state()->fd >= 0);
    assert(ost_log_state()->syslog_opened == false);
    ost_log_reset_for_tests();

    assert_file_contains(path, "file after syslog");
    unlink(path);
    clear_log_env();
}

static void test_file_backend_overrides_existing_disabled_backend(void)
{
    char path[128];

    clear_log_env();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(false);

    ost_log_write(LOG_LEVEL_INFO, "unit.c", 72, "disabled before file");
    assert(ost_log_state()->backend == OST_LOG_BACKEND_DISABLED);

    build_tmp_log_path(path, sizeof(path), "disabled_to_file");
    assert(setenv("OST_LOG_FILE_PATH", path, 1) == 0);

    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 73, "file after disabled");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_FILE);
    assert(ost_log_state()->fd >= 0);

    ost_log_reset_for_tests();
    unlink(path);
    clear_log_env();
}

static void test_callback_runs_without_state_mutex_held(void)
{
    clear_log_env();
    reset_callback_state();
    ost_log_reset_for_tests();

    assert(os_transport_log_reg(LOG_LEVEL_DEBUG, test_lock_probe_callback) == 0);
    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 70, "callback lock probe");

    assert(g_callback_saw_unlocked_mutex == true);
}

static void test_callback_backend_ignores_later_file_env(void)
{
    char path[128];

    clear_log_env();
    reset_callback_state();
    ost_log_reset_for_tests();

    build_tmp_log_path(path, sizeof(path), "late_env");

    assert(os_transport_log_reg(LOG_LEVEL_DEBUG, test_log_callback) == 0);
    assert(setenv("OST_LOG_FILE_PATH", path, 1) == 0);
    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 71, "callback despite env");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_CALLBACK);
    assert(ost_log_state()->fd == -1);
    assert(g_callback_calls == 1);
    assert(strstr(g_callback_msg, "callback despite env") != NULL);

    unlink(path);
    clear_log_env();
}

static void test_log_level_filter_applies_to_callback_backend(void)
{
    clear_log_env();
    reset_callback_state();
    ost_log_reset_for_tests();

    assert(os_transport_log_reg(LOG_LEVEL_ERROR, test_log_callback) == 0);
    ost_log_write(LOG_LEVEL_INFO, "unit.c", 72, "filtered info message");
    assert(g_callback_calls == 0);

    ost_log_write(LOG_LEVEL_ERROR, "unit.c", 73, "visible error message");
    assert(g_callback_calls == 1);
    assert(g_callback_level == LOG_LEVEL_ERROR);
    assert(strstr(g_callback_msg, "visible error message") != NULL);
}

static void test_no_default_file_fallback_without_file_env(void)
{
    clear_log_env();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(false);

    ost_log_write(LOG_LEVEL_INFO, "unit.c", 56, "disabled message");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_DISABLED);
    assert(ost_log_state()->fd == -1);
    clear_log_env();
}

static void test_syslog_backend_when_available(void)
{
    clear_log_env();
    ost_log_reset_for_tests();
    ost_log_force_syslog_available_for_tests(true);

    ost_log_write(LOG_LEVEL_INFO, "unit.c", 78, "syslog message");

    assert(ost_log_state()->backend == OST_LOG_BACKEND_SYSLOG);
    assert(ost_log_state()->syslog_opened == true);
}

int main(void)
{
    test_log_format_uses_file_argument();
    test_file_backend_takes_priority_over_syslog();
    test_callback_registration_overrides_existing_file_backend();
    test_callback_backend_omits_process_context();
    test_callback_registration_overrides_existing_syslog_backend();
    test_file_backend_overrides_existing_syslog_backend();
    test_file_backend_overrides_existing_disabled_backend();
    test_callback_runs_without_state_mutex_held();
    test_callback_backend_ignores_later_file_env();
    test_log_level_filter_applies_to_callback_backend();
    test_no_default_file_fallback_without_file_env();
    test_syslog_backend_when_available();

    printf("test_os_transport_log_unit passed\n");
    return 0;
}

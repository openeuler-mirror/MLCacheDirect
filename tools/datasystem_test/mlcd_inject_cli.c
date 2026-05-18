/**
 * mlcd_inject_cli - MLCacheDirect 故障注入命令行工具
 *
 * 部署在所有 Worker 节点上，用于远程设置故障注入点
 * 被 pipeline_h2d_fault_test 通过 SSH 调用
 *
 * 编译: gcc -o mlcd_inject_cli mlcd_inject_cli.c -I<MLCacheDirect>/include -L<MLCacheDirect>/lib -los_transport
 * -Wl,-rpath,<MLCacheDirect>/lib 或静态链接: gcc -o mlcd_inject_cli mlcd_inject_cli.c
 * <MLCacheDirect>/lib/libos_transport.a
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>
#include <errno.h>
#include <limits.h>

// 辅助函数：检查字符串是否为有效数字
static int is_valid_number(const char *str)
{
    if (!str || *str == '\0')
        return 0;
    char *endptr;
    errno = 0;
    long val = strtol(str, &endptr, 10);
    (void)val; // 抑制未使用警告
    return (*endptr == '\0') && (errno == 0);
}

// 辅助函数：安全转换字符串为 int
static int safe_atoi(const char *str, int *out_val)
{
    if (!is_valid_number(str))
        return -1;
    char *endptr;
    long val = strtol(str, &endptr, 10);
    if (val < INT_MIN || val > INT_MAX)
        return -1;
    *out_val = (int)val;
    return 0;
}

// 必须定义此宏才能看到共享内存结构
#define OS_TRANSPORT_WITH_INJECT 1
#include "os_transport_inject.h"

// 返回值定义（假设，根据实际库调整）
#define OS_TRANSPORT_INJECT_OK                0
#define OS_TRANSPORT_INJECT_ERR_NOT_FOUND     -1
#define OS_TRANSPORT_INJECT_ERR_INVALID_PARAM -2

// 打印使用帮助
static void print_usage(const char *prog)
{
    printf("Usage: %s <command> [options]\n", prog);
    printf("\nCommands:\n");
    printf("  set <point> sleep <ms>          Set sleep injection\n");
    printf("  set <point> error <code>        Set error return injection\n");
    printf("  clear <point>                   Clear specific injection point\n");
    printf("  clearall                        Clear all injection points\n");
    printf("  list                            List available injection points\n");
    printf("  verify <point> <seconds>        Verify injection was triggered recently\n");
    printf("\nInjection Points:\n");
    printf("  os_transport.recv.begin         Flow 4: Receive service start\n");
    printf("  os_transport.urma.recv          Flow 4: URMA recv WR post\n");
    printf("  os_transport.send.begin         Flow 6: Send service start\n");
    printf("  os_transport.send.first_chunk   Flow 6: First chunk URMA write\n");
    printf("  os_transport.urma.write         Flow 7: URMA write operation\n");
    printf("  os_transport.notify_callback    Flow 8: notify_callback\n");
    printf("  os_transport.task_group.alloc   Flow 4/6: Task group allocation\n");
    printf("\nExamples:\n");
    printf("  %s set os_transport.recv.begin sleep 3000\n", prog);
    printf("  %s set os_transport.send.begin error 1\n", prog);
    printf("  %s set os_transport.urma.write sleep 100\n", prog);
    printf("  %s set os_transport.notify_callback error 999\n", prog);
    printf("  %s clear os_transport.recv.begin\n", prog);
    printf("  %s clearall\n", prog);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        print_usage(argv[0]);
        return 1;
    }

    // 检查环境变量启用调试模式
    const char *debug_env = getenv("MLCD_INJECT_DEBUG");
    int debug_mode = (debug_env && (strcmp(debug_env, "1") == 0 || strcmp(debug_env, "true") == 0));

    if (debug_mode) {
        fprintf(stderr, "[mlcd_inject] Debug mode enabled\n");
        fprintf(stderr, "[mlcd_inject] argc=%d\n", argc);
        for (int i = 0; i < argc; i++) {
            fprintf(stderr, "[mlcd_inject] argv[%d]=%s\n", i, argv[i]);
        }
    }

    const char *cmd = argv[1];

    // 处理帮助命令
    if (strcmp(cmd, "--help") == 0 || strcmp(cmd, "-h") == 0 || strcmp(cmd, "help") == 0) {
        print_usage(argv[0]);
        return 0;
    }

    // list 命令
    if (strcmp(cmd, "list") == 0) {
        printf("Available injection points:\n");
        printf("  os_transport.recv.begin       (Flow 4: Receive service)\n");
        printf("  os_transport.urma.recv        (Flow 4: URMA recv WR post)\n");
        printf("  os_transport.send.begin       (Flow 6: Send service)\n");
        printf("  os_transport.send.first_chunk (Flow 6: First chunk URMA write)\n");
        printf("  os_transport.urma.write       (Flow 7: URMA write)\n");
        printf("  os_transport.notify_callback  (Flow 8: notify_callback)\n");
        printf("  os_transport.task_group.alloc (Flow 4/6: Task group allocation)\n");
        return 0;
    }

    // clearall 命令
    if (strcmp(cmd, "clearall") == 0) {
        printf("[mlcd_inject] Clearing all injection points\n");
        os_transport_inject_clear_all();
        printf("[mlcd_inject] All injection points cleared successfully\n");
        return 0;
    }

    // set 命令: set <point> <type> <value>
    if (strcmp(cmd, "set") == 0) {
        if (argc < 5) {
            fprintf(stderr, "Error: 'set' requires 4 arguments: set <point> <type> <value>\n");
            return 1;
        }

        const char *point = argv[2];
        const char *type = argv[3];
        int value;

        // 验证数值参数
        if (safe_atoi(argv[4], &value) != 0) {
            fprintf(stderr, "Error: Invalid numeric value '%s'\n", argv[4]);
            return 1;
        }

        // 校验注入点名称（基本的合法性检查）
        if (strncmp(point, "os_transport.", 13) != 0) {
            fprintf(stderr, "Warning: Injection point '%s' does not start with 'os_transport.'\n", point);
        }

        if (strcmp(type, "sleep") == 0) {
            // 检查负数
            if (value < 0) {
                fprintf(stderr, "Error: Sleep time cannot be negative (%d)\n", value);
                return 1;
            }
            printf("[mlcd_inject] Set %s = sleep(%d ms)\n", point, value);
            int ret = os_transport_inject_set_sleep(point, (uint32_t)value);
            if (ret != 0) {
                fprintf(stderr, "Error: Failed to set sleep injection, ret=%d\n", ret);
                if (debug_mode) {
                    fprintf(stderr, "[mlcd_inject] Hint: Check if injection point '%s' exists\n", point);
                }
                return 1;
            }
            if (debug_mode) {
                fprintf(stderr, "[mlcd_inject] Sleep injection set successfully\n");
            }
        } else if (strcmp(type, "error") == 0) {
            printf("[mlcd_inject] Set %s = return_error(%d)\n", point, value);
            int ret = os_transport_inject_set_return_error(point, value);
            if (ret != 0) {
                fprintf(stderr, "Error: Failed to set error injection, ret=%d\n", ret);
                if (debug_mode) {
                    fprintf(stderr, "[mlcd_inject] Hint: Check if injection point '%s' exists\n", point);
                }
                return 1;
            }
            if (debug_mode) {
                fprintf(stderr, "[mlcd_inject] Error injection set successfully\n");
            }
        } else {
            fprintf(stderr, "Error: Unknown type '%s', use 'sleep' or 'error'\n", type);
            return 1;
        }
        return 0;
    }

    // verify 命令: verify <point> <seconds_ago>
    if (strcmp(cmd, "verify") == 0) {
        if (argc < 4) {
            fprintf(stderr, "Error: 'verify' requires 2 arguments: verify <point> <seconds_ago>\n");
            return 1;
        }
        const char *point = argv[2];
        int sec;
        if (safe_atoi(argv[3], &sec) != 0 || sec < 0) {
            fprintf(stderr, "Error: Invalid seconds value '%s'\n", argv[3]);
            return 1;
        }

        os_transport_inject_registry_t *shm = os_transport_inject_init();
        if (!shm) {
            printf("NOT_TRIGGERED\n");
            return 1;
        }

        struct timespec ts;
        clock_gettime(CLOCK_MONOTONIC, &ts);
        uint64_t now_ns = (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
        uint64_t threshold = (uint64_t)sec * 1000000000ULL;

        uint32_t tail = __atomic_load_n(&shm->ring_tail, __ATOMIC_ACQUIRE);
        for (int i = 0; i < OS_TRANSPORT_INJECT_RING_SIZE; i++) {
            int idx = (int)(tail - 1 - i + OS_TRANSPORT_INJECT_RING_SIZE) % OS_TRANSPORT_INJECT_RING_SIZE;
            if (strcmp(shm->ring[idx].name, point) == 0) {
                if (now_ns - shm->ring[idx].timestamp_ns <= threshold) {
                    printf("TRIGGERED\n");
                    return 0;
                }
            }
        }
        printf("NOT_TRIGGERED\n");
        return 1;
    }

    // clear 命令: clear <point>
    if (strcmp(cmd, "clear") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Error: 'clear' requires point name\n");
            return 1;
        }
        const char *point = argv[2];
        printf("[mlcd_inject] Clearing injection point: %s\n", point);
        os_transport_inject_clear(point);
        printf("[mlcd_inject] Injection point '%s' cleared successfully\n", point);
        return 0;
    }

    fprintf(stderr, "Error: Unknown command '%s'\n", cmd);
    print_usage(argv[0]);
    return 1;
}

/**
 * Pipeline H2D Fault Injection Test (Simplified)
 *
 * 仅覆盖 MLCacheDirect 层的4个流程：
 * - 流程4: 接收服务启动 (os_transport_recv)
 * - 流程6: 发送服务启动 (os_transport_send)
 * - 流程7: URMA写操作 (urma_post_jfs_wr)
 * - 流程8: notify_callback
 */

#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include <iomanip>
#include <vector>
#include <sstream>
#include <string>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <cuda_runtime.h>
#include <chrono>
#include <random>
#include <atomic>
#include <algorithm>
#include <thread>
#include <numeric>
#include <mutex>
#include <map>
#include <functional>
#include <dlfcn.h>
#include <unistd.h> // for access()

using namespace datasystem;

std::vector<std::string> SplitString(const std::string &str, char delimiter)
{
    std::vector<std::string> tokens;
    std::stringstream ss(str);
    std::string token;
    while (std::getline(ss, token, delimiter)) {
        if (!token.empty()) {
            tokens.push_back(token);
        }
    }
    return tokens;
}

bool ParseKeyValue(const std::string &arg, std::string &key, std::string &value)
{
    if (arg.size() < 3 || arg[0] != '-' || arg[1] != '-') {
        return false;
    }
    size_t eq_pos = arg.find('=');
    if (eq_pos == std::string::npos) {
        return false;
    }
    key = arg.substr(2, eq_pos - 2);
    value = arg.substr(eq_pos + 1);
    return true;
}

bool ParseBool(const std::string &str)
{
    if (str == "true" || str == "1")
        return true;
    if (str == "false" || str == "0")
        return false;
    return true;
}

uint32_t ResolveInjectDelayMs(int custom_delay_ms, uint32_t default_delay_ms)
{
    return (custom_delay_ms >= 0) ? static_cast<uint32_t>(custom_delay_ms) : default_delay_ms;
}

uint32_t VerifyWindowSecondsFromLatency(long long latency_us)
{
    if (latency_us <= 0) {
        return 1;
    }
    return static_cast<uint32_t>((latency_us + 999999) / 1000000) + 1;
}

// 异常测试场景定义（仅流程4、6、7、8）
enum class FaultScenario {
    NO_FAULT = 0, // 正常流程
    // 流程4: os_transport_recv启动接收服务
    RECV_SERVICE_START_DELAY, // 流程4: 接收服务启动延迟
    RECV_SERVICE_START_ERROR, // 流程4: 接收服务启动错误
    // 流程6: os_transport_send启动发送服务
    SEND_SERVICE_START_DELAY, // 流程6: 发送服务启动延迟
    SEND_SERVICE_START_ERROR, // 流程6: 发送服务启动错误
    // 流程7: URMA写操作
    URMA_WRITE_DELAY, // 流程7: URMA写延迟
    URMA_WRITE_ERROR, // 流程7: URMA写错误
    // 流程8: notify_callback
    NOTIFY_CALLBACK_DELAY, // 流程8: notify_callback延迟
    NOTIFY_CALLBACK_ERROR, // 流程8: notify_callback错误
    // 组合场景
    MLCD_FAULT_CHAIN, // MLCacheDirect层多故障组合
    // 补充场景（覆盖内部错误分支）
    URMA_RECV_ERROR = 20,        // 流程4内部: URMA recv WR提交失败
    SEND_FIRST_CHUNK_ERROR = 21, // 流程6内部: 主线程第0个chunk发送失败
    TASK_GROUP_ALLOC_ERROR = 22, // 流程4/6内部: 线程池任务注册/内存分配失败
};

// 预期结果定义
enum class ExpectedResult {
    SUCCESS,                       // 正常流程必须成功
    RPC_TIMEOUT_OR_DELAY_OBSERVED, // 延迟注入预期超时，或成功但耗时体现注入延迟
    KEYS_FAILED,                   // 错误注入预期部分/全部key失败
    ANY_ERROR,                     // 组合故障：只要出错就算符合预期
};

/*
 *对于延迟场景，预期的结果为:
 *1.RPC调用超时，返回失败（在注入的延迟超过超时阈值时）；
 *2.RPC调用成功且返回结果正确，但整体耗时明显增加（在注入的延迟未超过超时阈值时）；
 *因此延迟场景的用例通过判定为返回超时、或耗时大于注入的延迟。
 *
 *对于错误场景：
 *MGetH2D接口会尽可能保证返回成功，因此包含了多级的fallback机制，
 *实现上，该接口提供三种等级的传输方式，依次回滚：pipeline_h2d->原生urma_write+h2d->tcp+h2d，
 *不同的注入点会触发不同的fallback分支。
 *例如当接收侧调用失败时，会回落到原生urma_write+h2d，最终返回成功。
 *（对应RECV_SERVICE_START_ERROR、URMA_RECV_ERROR和TASK_GROUP_ALLOC_ERROR，此时向远端worker的rpc还未发出）。
 *当发送侧调用失败时，会回落到tcp+h2d，但由于未修改tcp传输阈值（1M，测试工具默认使用8M），因此也会返回失败。
 *（对应SEND_SERVICE_START_ERROR、URMA_WRITE_ERROR和SEND_FIRST_CHUNK_ERROR）
 *特别地，对于NOTIFY_CALLBACK_ERROR场景，实际数据已到达本地worker，
 *但注入错误点在本地worker调用notify_callback时（即recv侧task）失败，
 *因此会在wait_and_free_sync_timeout中等待到超时，最终返回失败。
 */
std::map<FaultScenario, ExpectedResult> g_expectedResult = {
    {FaultScenario::NO_FAULT, ExpectedResult::SUCCESS},
    {FaultScenario::RECV_SERVICE_START_DELAY, ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED},
    {FaultScenario::RECV_SERVICE_START_ERROR, ExpectedResult::SUCCESS},
    {FaultScenario::SEND_SERVICE_START_DELAY, ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED},
    {FaultScenario::SEND_SERVICE_START_ERROR, ExpectedResult::KEYS_FAILED},
    {FaultScenario::URMA_WRITE_DELAY, ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED},
    {FaultScenario::URMA_WRITE_ERROR, ExpectedResult::KEYS_FAILED},
    {FaultScenario::NOTIFY_CALLBACK_DELAY, ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED},
    {FaultScenario::NOTIFY_CALLBACK_ERROR, ExpectedResult::KEYS_FAILED},
    {FaultScenario::MLCD_FAULT_CHAIN, ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED},
    {FaultScenario::URMA_RECV_ERROR, ExpectedResult::SUCCESS},
    {FaultScenario::SEND_FIRST_CHUNK_ERROR, ExpectedResult::KEYS_FAILED},
    {FaultScenario::TASK_GROUP_ALLOC_ERROR, ExpectedResult::SUCCESS},
};

// 场景描述映射
std::map<FaultScenario, std::string> g_scenarioDesc = {
    {FaultScenario::NO_FAULT, "正常流程测试"},
    {FaultScenario::RECV_SERVICE_START_DELAY, "流程4: 接收服务启动延迟"},
    {FaultScenario::RECV_SERVICE_START_ERROR, "流程4: 接收服务启动错误"},
    {FaultScenario::SEND_SERVICE_START_DELAY, "流程6: 发送服务启动延迟"},
    {FaultScenario::SEND_SERVICE_START_ERROR, "流程6: 发送服务启动错误"},
    {FaultScenario::URMA_WRITE_DELAY, "流程7: URMA写延迟"},
    {FaultScenario::URMA_WRITE_ERROR, "流程7: URMA写错误"},
    {FaultScenario::NOTIFY_CALLBACK_DELAY, "流程8: notify_callback延迟"},
    {FaultScenario::NOTIFY_CALLBACK_ERROR, "流程8: notify_callback错误"},
    {FaultScenario::MLCD_FAULT_CHAIN, "MLCacheDirect层多故障组合"},
    {FaultScenario::URMA_RECV_ERROR, "流程4内部: URMA recv WR提交失败"},
    {FaultScenario::SEND_FIRST_CHUNK_ERROR, "流程6内部: 主线程第0个chunk发送失败"},
    {FaultScenario::TASK_GROUP_ALLOC_ERROR, "流程4/6内部: 线程池任务注册/内存分配失败"},
};

const char *ExpectedResultToString(ExpectedResult expected)
{
    switch (expected) {
    case ExpectedResult::SUCCESS:
        return "success";
    case ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED:
        return "rpc_timeout_or_delay_observed";
    case ExpectedResult::KEYS_FAILED:
        return "keys_failed";
    case ExpectedResult::ANY_ERROR:
        return "any_error";
    }
    return "unknown";
}

uint32_t ExpectedDelayMsForScenario(FaultScenario scenario, int inject_delay_ms)
{
    switch (scenario) {
    case FaultScenario::RECV_SERVICE_START_DELAY:
    case FaultScenario::SEND_SERVICE_START_DELAY:
        return ResolveInjectDelayMs(inject_delay_ms, 3000);
    case FaultScenario::URMA_WRITE_DELAY:
    case FaultScenario::NOTIFY_CALLBACK_DELAY:
        return ResolveInjectDelayMs(inject_delay_ms, 100);
    case FaultScenario::MLCD_FAULT_CHAIN:
        return ResolveInjectDelayMs(inject_delay_ms, 500);
    default:
        return 0;
    }
}

// MLCacheDirect注入点函数类型
typedef int (*os_transport_inject_set_sleep_fn)(const char *, uint32_t);
typedef int (*os_transport_inject_set_return_error_fn)(const char *, int);
typedef void (*os_transport_inject_clear_fn)(const char *);
typedef void (*os_transport_inject_clear_all_fn)(void);

// MLCacheDirect注入点管理器
class MLCacheDirectInjectManager {
public:
    MLCacheDirectInjectManager() :
        initialized_(false), handle_(nullptr), set_sleep_fn_(nullptr), set_return_error_fn_(nullptr),
        clear_fn_(nullptr), clear_all_fn_(nullptr)
    {
        Init();
    }

    ~MLCacheDirectInjectManager()
    {
        if (handle_) {
            dlclose(handle_);
        }
    }

    bool IsAvailable() const
    {
        return initialized_;
    }

    bool SetSleep(const char *name, uint32_t ms)
    {
        if (!set_sleep_fn_) {
            std::cerr << "[MLCacheDirect] Error: SetSleep function not available" << std::endl;
            return false;
        }
        int ret = set_sleep_fn_(name, ms);
        if (ret == 0) {
            std::cout << "[MLCacheDirect] Inject point set: " << name << " = sleep(" << ms << "ms)" << std::endl;
            return true;
        } else {
            std::cerr << "[MLCacheDirect] Error: Failed to set sleep injection for " << name << ", ret=" << ret
                      << std::endl;
            return false;
        }
    }

    bool SetReturnError(const char *name, int error_code)
    {
        if (!set_return_error_fn_) {
            std::cerr << "[MLCacheDirect] Error: SetReturnError function not available" << std::endl;
            return false;
        }
        int ret = set_return_error_fn_(name, error_code);
        if (ret == 0) {
            std::cout << "[MLCacheDirect] Inject point set: " << name << " = return_error(" << error_code << ")"
                      << std::endl;
            return true;
        } else {
            std::cerr << "[MLCacheDirect] Error: Failed to set error injection for " << name << ", ret=" << ret
                      << std::endl;
            return false;
        }
    }

    bool Clear(const char *name)
    {
        if (!clear_fn_) {
            return false;
        }
        clear_fn_(name);
        std::cout << "[MLCacheDirect] Inject point cleared: " << name << std::endl;
        return true;
    }

    bool ClearAll()
    {
        if (!clear_all_fn_) {
            return false;
        }
        clear_all_fn_();
        std::cout << "[MLCacheDirect] All inject points cleared" << std::endl;
        return true;
    }

    // MLCacheDirect层注入点名称
    static const char *INJECT_RECV_BEGIN()
    {
        return "os_transport.recv.begin";
    }
    static const char *INJECT_SEND_BEGIN()
    {
        return "os_transport.send.begin";
    }
    static const char *INJECT_URMA_WRITE()
    {
        return "os_transport.urma.write";
    }
    static const char *INJECT_NOTIFY_CALLBACK()
    {
        return "os_transport.notify_callback";
    }
    static const char *INJECT_URMA_RECV()
    {
        return "os_transport.urma.recv";
    }
    static const char *INJECT_SEND_FIRST_CHUNK()
    {
        return "os_transport.send.first_chunk";
    }
    static const char *INJECT_ALLOC_TASK_GROUP()
    {
        return "os_transport.task_group.alloc";
    }

private:
    // 自动查找 libos_transport.so 库路径
    std::string FindLibraryPath()
    {
        // 1. 检查环境变量
        const char *env_path = getenv("MLCACHEDIRECT_LIB_PATH");
        if (env_path) {
            std::string full_path = std::string(env_path) + "/libos_transport.so";
            if (access(full_path.c_str(), F_OK) == 0) {
                return full_path;
            }
        }

        // 2. 尝试常见安装路径
        const char *common_paths[] = {
            "/usr/local/lib/python3.11/site-packages/yr/datasystem/lib/libos_transport.so",   // Python包安装路径1
            "/usr/local/lib64/python3.11/site-packages/yr/datasystem/lib/libos_transport.so", // Python包安装路径2
            "./libos_transport.so",                                                           // 当前目录
            "../lib/libos_transport.so",                                                      // 相对路径
            "/usr/local/lib/libos_transport.so",                                              // 标准安装路径
            "/usr/lib/libos_transport.so",                                                    // 系统库路径
            "/usr/lib64/libos_transport.so",                                                  // 64位系统路径
            "/opt/MLCacheDirect/lib/libos_transport.so",                                      // 默认安装路径
            "/opt/mlcachedirect/lib/libos_transport.so",                                      // 小写路径
            nullptr};

        for (int i = 0; common_paths[i]; ++i) {
            if (access(common_paths[i], F_OK) == 0) {
                return common_paths[i];
            }
        }

        // 3. 尝试从 LD_LIBRARY_PATH 中查找
        const char *ld_path = getenv("LD_LIBRARY_PATH");
        if (ld_path) {
            std::stringstream ss(ld_path);
            std::string path;
            while (std::getline(ss, path, ':')) {
                std::string full_path = path + "/libos_transport.so";
                if (access(full_path.c_str(), F_OK) == 0) {
                    return full_path;
                }
            }
        }

        // 4. 返回默认名称（依赖系统动态链接器）
        return "libos_transport.so";
    }

    void Init()
    {
        std::string lib_path = FindLibraryPath();
        handle_ = dlopen(lib_path.c_str(), RTLD_LAZY | RTLD_GLOBAL);
        if (!handle_) {
            std::cerr << "[MLCacheDirect] Warning: Cannot load libos_transport.so: " << dlerror() << std::endl;
            std::cerr
                << "[MLCacheDirect] Hint: Set MLCACHEDIRECT_LIB_PATH or LD_LIBRARY_PATH to the directory containing libos_transport.so"
                << std::endl;
            return;
        }
        std::cout << "[MLCacheDirect] Loaded from: " << lib_path << std::endl;

        set_sleep_fn_ = (os_transport_inject_set_sleep_fn)dlsym(handle_, "os_transport_inject_set_sleep");
        set_return_error_fn_ =
            (os_transport_inject_set_return_error_fn)dlsym(handle_, "os_transport_inject_set_return_error");
        clear_fn_ = (os_transport_inject_clear_fn)dlsym(handle_, "os_transport_inject_clear");
        clear_all_fn_ = (os_transport_inject_clear_all_fn)dlsym(handle_, "os_transport_inject_clear_all");

        if (set_sleep_fn_ && set_return_error_fn_ && clear_fn_ && clear_all_fn_) {
            initialized_ = true;
            std::cout << "[MLCacheDirect] Fault injection interface loaded successfully" << std::endl;
        } else {
            std::cerr << "[MLCacheDirect] Warning: Some inject functions not found" << std::endl;
        }
    }

    bool initialized_;
    void *handle_;
    os_transport_inject_set_sleep_fn set_sleep_fn_;
    os_transport_inject_set_return_error_fn set_return_error_fn_;
    os_transport_inject_clear_fn clear_fn_;
    os_transport_inject_clear_all_fn clear_all_fn_;
};

// CUDA 内存自动释放器（RAII）
class CudaMemoryGuard {
public:
    explicit CudaMemoryGuard(std::vector<std::pair<void *, size_t>> &chunks) : chunks_(chunks)
    {}

    ~CudaMemoryGuard()
    {
        for (auto &chunk : chunks_) {
            if (chunk.first) {
                cudaFree(chunk.first);
                chunk.first = nullptr;
            }
        }
    }

    // 释放后禁用自动清理（当正常流程已手动释放时调用）
    void Release()
    {
        for (auto &chunk : chunks_) {
            chunk.first = nullptr;
        }
    }

private:
    std::vector<std::pair<void *, size_t>> &chunks_;
};

// 性能统计结构体
struct FaultTestStats {
    std::atomic<uint64_t> total_requests{0};
    std::atomic<uint64_t> passed_requests{0};
    std::atomic<uint64_t> failed_requests{0};
    std::vector<long long> latencies;
    std::chrono::steady_clock::time_point start_time;
    std::mutex latencies_mutex;

    void RecordLatency(long long latency_us)
    {
        std::lock_guard<std::mutex> lock(latencies_mutex);
        latencies.push_back(latency_us);
    }

    void PrintSummary(const std::string &scenarioName)
    {
        auto end_time = std::chrono::steady_clock::now();
        auto total_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
        double total_duration_sec = total_duration_us / 1000000.0;

        uint64_t total = total_requests.load();
        uint64_t passed = passed_requests.load();
        uint64_t failed = failed_requests.load();

        std::cout << "\n========== [Fault Test Summary: " << scenarioName << "] ==========" << std::endl;
        std::cout << "Total Requests: " << total << std::endl;
        std::cout << "Passed: " << passed << std::endl;
        std::cout << "Failed: " << failed << std::endl;
        std::cout << "Duration: " << std::fixed << std::setprecision(3) << total_duration_sec << "s" << std::endl;

        if (!latencies.empty()) {
            std::sort(latencies.begin(), latencies.end());
            auto min_lat = latencies.front();
            auto max_lat = latencies.back();
            size_t p50_idx = latencies.size() * 50 / 100;
            size_t p99_idx = latencies.size() * 99 / 100;
            p50_idx = std::min(p50_idx, latencies.size() - 1);
            p99_idx = std::min(p99_idx, latencies.size() - 1);
            auto p50 = latencies[p50_idx];
            auto p99 = latencies[p99_idx];
            auto avg = std::accumulate(latencies.begin(), latencies.end(), 0LL) / latencies.size();

            std::cout << "\nLatency (us):" << std::endl;
            std::cout << "  Min: " << min_lat << std::endl;
            std::cout << "  Avg: " << avg << std::endl;
            std::cout << "  P50: " << p50 << std::endl;
            std::cout << "  P99: " << p99 << std::endl;
            std::cout << "  Max: " << max_lat << std::endl;
        }
        std::cout << "=========================================\n" << std::endl;
    }
};

/**
 * SSH 远程注入执行器
 * 用于在远程 Worker 上执行 mlcd_inject_cli 设置注入点
 */
class SSHRemoteInjector {
public:
    SSHRemoteInjector(const std::string &host, const std::string &user = "") :
        host_(host), user_(user.empty() ? "" : user + "@")
    {}

    // 设置远程注入点
    bool SetSleep(const std::string &point, uint32_t ms)
    {
        std::string cmd = BuildCommand("set", point, "sleep", std::to_string(ms));
        std::cout << "[SSHRemote] Setting sleep injection on " << host_ << ": " << point << "=" << ms << "ms"
                  << std::endl;
        if (!ExecuteSSH(cmd)) {
            std::cerr << "[SSHRemote] Error: Failed to set sleep injection on " << host_ << std::endl;
            return false;
        }
        return true;
    }

    bool SetReturnError(const std::string &point, int error_code)
    {
        std::string cmd = BuildCommand("set", point, "error", std::to_string(error_code));
        std::cout << "[SSHRemote] Setting error injection on " << host_ << ": " << point << "=" << error_code
                  << std::endl;
        if (!ExecuteSSH(cmd)) {
            std::cerr << "[SSHRemote] Error: Failed to set error injection on " << host_ << std::endl;
            return false;
        }
        return true;
    }

    bool Clear(const std::string &point)
    {
        std::string cmd = "mlcd_inject_cli clear " + point;
        std::cout << "[SSHRemote] Clearing injection on " << host_ << ": " << point << std::endl;
        if (!ExecuteSSH(cmd)) {
            std::cerr << "[SSHRemote] Warning: Failed to clear injection on " << host_ << std::endl;
            return false;
        }
        return true;
    }

    bool ClearAll()
    {
        std::string cmd = "mlcd_inject_cli clearall";
        std::cout << "[SSHRemote] Clearing all injections on " << host_ << std::endl;
        if (!ExecuteSSH(cmd)) {
            std::cerr << "[SSHRemote] Warning: Failed to clear all injections on " << host_ << std::endl;
            return false;
        }
        return true;
    }

    bool IsAvailable() const
    {
        return !host_.empty();
    }

private:
    std::string host_;
    std::string user_;

    std::string
    BuildCommand(const std::string &action, const std::string &point, const std::string &type, const std::string &value)
    {
        return "mlcd_inject_cli " + action + " " + point + " " + type + " " + value;
    }

    bool ExecuteSSH(const std::string &cmd)
    {
        std::string full_cmd = "ssh -o BatchMode=yes -o ConnectTimeout=5 " + user_ + host_ + " \"" + cmd + "\"";
        int ret = system(full_cmd.c_str());
        return (ret == 0);
    }
};

/**
 * H2D 故障注入测试类
 *
 * 支持两种模式：
 * 1. 单机模式：只设置本地注入点（流程4/6/7/8都在本地）
 * 2. 集群模式：本地设置流程4/8，远程 SSH 设置流程6/7
 */
class H2DFaultTest {
public:
    // host: 本地 Worker (Get端，流程4/8)
    // remote_worker: 远程 Worker (Set端，流程6/7)，为空表示单机模式
    H2DFaultTest(const std::string &host, const std::string &remote_worker = "", int port = 18481, int gpu_id = 0) :
        host_(host), port_(port), gpu_id_(gpu_id), remote_worker_(remote_worker), mlcd_inject_(),
        remote_inject_(remote_worker), is_cluster_(!remote_worker.empty())
    {
        ResetClient();
    }

    void SetDataParams(const std::string &value_prefix,
                       const std::vector<std::string> &custom_keys,
                       int count,
                       size_t value_size,
                       bool delete_value)
    {
        if (value_prefix.empty()) {
            base_value_prefix_ = "0";
        } else if (value_prefix.size() == 1) {
            base_value_prefix_ = value_prefix;
        } else {
            base_value_prefix_ = value_prefix.substr(0, 1);
            std::cout << "[WARN] value_prefix truncated to 1 char: '" << base_value_prefix_ << "'" << std::endl;
        }

        custom_keys_ = custom_keys;
        value_size_ = (value_size > 0) ? value_size : 8388608;
        delete_value_ = delete_value;

        if (count <= 0 && !custom_keys_.empty()) {
            count_ = static_cast<int>(custom_keys_.size());
        } else {
            count_ = (count > 0) ? count : 10;
        }
    }

    void ResetClient()
    {
        ConnectOptions connectOptions;
        connectOptions.host = host_;
        connectOptions.port = port_;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(gpu_id_);

        client_ = std::make_unique<KVClient>(connectOptions);
        client_->Init();
    }

    // 设置故障场景
    bool SetFaultScenario(FaultScenario scenario, int inject_delay_ms = -1)
    {
        // 先清除所有注入点
        mlcd_inject_.ClearAll();
        if (is_cluster_) {
            remote_inject_.ClearAll();
        }

        bool all_success = true;

        switch (scenario) {
        case FaultScenario::NO_FAULT:
            // 正常流程，不需要设置任何注入点
            std::cout << "[FaultScenario] Running normal flow (no fault injection)" << std::endl;
            break;
        case FaultScenario::RECV_SERVICE_START_DELAY:
            // 流程4: 本地注入延迟
            all_success &=
                mlcd_inject_.SetSleep(mlcd_inject_.INJECT_RECV_BEGIN(), ResolveInjectDelayMs(inject_delay_ms, 3000));
            break;
        case FaultScenario::RECV_SERVICE_START_ERROR:
            // 流程4: 本地注入 (返回错误码 1)
            all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_RECV_BEGIN(), 1);
            break;
        case FaultScenario::SEND_SERVICE_START_DELAY:
            // 流程6: 集群模式下远程注入，单机模式下本地注入延迟
            if (is_cluster_) {
                all_success &=
                    remote_inject_.SetSleep("os_transport.send.begin", ResolveInjectDelayMs(inject_delay_ms, 3000));
            } else {
                all_success &= mlcd_inject_.SetSleep(mlcd_inject_.INJECT_SEND_BEGIN(),
                                                     ResolveInjectDelayMs(inject_delay_ms, 3000));
            }
            break;
        case FaultScenario::SEND_SERVICE_START_ERROR:
            // 流程6: 集群模式下远程注入，单机模式下本地注入 (返回错误码 1)
            if (is_cluster_) {
                all_success &= remote_inject_.SetReturnError("os_transport.send.begin", 1);
            } else {
                all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_SEND_BEGIN(), 1);
            }
            break;
        case FaultScenario::URMA_WRITE_DELAY:
            // 流程7: 集群模式下远程注入，单机模式下本地注入延迟
            if (is_cluster_) {
                all_success &=
                    remote_inject_.SetSleep("os_transport.urma.write", ResolveInjectDelayMs(inject_delay_ms, 100));
            } else {
                all_success &=
                    mlcd_inject_.SetSleep(mlcd_inject_.INJECT_URMA_WRITE(), ResolveInjectDelayMs(inject_delay_ms, 100));
            }
            break;
        case FaultScenario::URMA_WRITE_ERROR:
            // 流程7: 集群模式下远程注入，单机模式下本地注入 (URMA_FAIL = 0x1000)
            if (is_cluster_) {
                all_success &= remote_inject_.SetReturnError("os_transport.urma.write", 0x1000);
            } else {
                all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_URMA_WRITE(), 0x1000);
            }
            break;
        case FaultScenario::NOTIFY_CALLBACK_DELAY:
            // 流程8: notify_callback本地注入延迟
            all_success &= mlcd_inject_.SetSleep(mlcd_inject_.INJECT_NOTIFY_CALLBACK(),
                                                 ResolveInjectDelayMs(inject_delay_ms, 100));
            break;
        case FaultScenario::NOTIFY_CALLBACK_ERROR:
            // 流程8: notify_callback本地注入错误
            all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_NOTIFY_CALLBACK(), 999);
            break;
        case FaultScenario::MLCD_FAULT_CHAIN:
            // 组合场景: 所有流程都注入延迟
            all_success &=
                mlcd_inject_.SetSleep(mlcd_inject_.INJECT_RECV_BEGIN(), ResolveInjectDelayMs(inject_delay_ms, 500));
            all_success &=
                mlcd_inject_.SetSleep(mlcd_inject_.INJECT_NOTIFY_CALLBACK(), ResolveInjectDelayMs(inject_delay_ms, 50));
            if (is_cluster_) {
                all_success &=
                    remote_inject_.SetSleep("os_transport.send.begin", ResolveInjectDelayMs(inject_delay_ms, 500));
                all_success &=
                    remote_inject_.SetSleep("os_transport.urma.write", ResolveInjectDelayMs(inject_delay_ms, 50));
            } else {
                all_success &=
                    mlcd_inject_.SetSleep(mlcd_inject_.INJECT_SEND_BEGIN(), ResolveInjectDelayMs(inject_delay_ms, 500));
                all_success &=
                    mlcd_inject_.SetSleep(mlcd_inject_.INJECT_URMA_WRITE(), ResolveInjectDelayMs(inject_delay_ms, 50));
            }
            break;
        case FaultScenario::URMA_RECV_ERROR:
            // 流程4内部: 本地注入 (URMA_FAIL = 0x1000)
            all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_URMA_RECV(), 0x1000);
            break;
        case FaultScenario::SEND_FIRST_CHUNK_ERROR:
            // 流程6内部: 集群模式下远程注入，单机模式下本地注入 (URMA_FAIL = 0x1000)
            if (is_cluster_) {
                all_success &= remote_inject_.SetReturnError("os_transport.send.first_chunk", 0x1000);
            } else {
                all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_SEND_FIRST_CHUNK(), 0x1000);
            }
            break;
        case FaultScenario::TASK_GROUP_ALLOC_ERROR:
            // 流程4/6内部: 本地注入 (返回错误码 1)
            // 注意：共享内存全局可见，集群模式下远端worker也会触发
            all_success &= mlcd_inject_.SetReturnError(mlcd_inject_.INJECT_ALLOC_TASK_GROUP(), 1);
            break;
        default:
            std::cerr << "[FaultScenario] Warning: Unknown scenario" << std::endl;
            return false;
        }

        return all_success;
    }

    // 清除故障场景
    void ClearFaultScenario()
    {
        mlcd_inject_.ClearAll();
        if (is_cluster_) {
            remote_inject_.ClearAll();
        }
    }

    // 检查故障注入是否可用
    bool IsFaultInjectionAvailable() const
    {
        return mlcd_inject_.IsAvailable();
    }

    // 检查测试结果是否符合预期
    static bool CheckResult(FaultScenario scenario,
                            const Status &ret,
                            const std::vector<std::string> &outFailedKeys,
                            long long latency_us,
                            int inject_delay_ms)
    {
        auto it = g_expectedResult.find(scenario);
        if (it == g_expectedResult.end())
            return false;
        switch (it->second) {
        case ExpectedResult::SUCCESS:
            return ret.IsOk() && outFailedKeys.empty();
        case ExpectedResult::RPC_TIMEOUT_OR_DELAY_OBSERVED: {
            if (ret.GetCode() == StatusCode::K_RPC_DEADLINE_EXCEEDED) {
                return true;
            }
            uint32_t expected_delay_ms = ExpectedDelayMsForScenario(scenario, inject_delay_ms);
            long long expected_delay_us = static_cast<long long>(expected_delay_ms) * 1000;
            return ret.IsOk() && outFailedKeys.empty() && latency_us >= expected_delay_us;
        }
        case ExpectedResult::KEYS_FAILED:
            return !ret.IsOk() || !outFailedKeys.empty();
        case ExpectedResult::ANY_ERROR:
            return !ret.IsOk() || !outFailedKeys.empty();
        }
        return false;
    }

    // 通过 mlcd_inject_cli verify 确认注入点确实触发
    bool VerifyInjection(FaultScenario scenario, long long latency_us) const
    {
        if (scenario == FaultScenario::NO_FAULT)
            return true;
        std::string point;
        bool is_remote = false;
        uint32_t verify_seconds = VerifyWindowSecondsFromLatency(latency_us);
        switch (scenario) {
        case FaultScenario::RECV_SERVICE_START_DELAY:
            point = mlcd_inject_.INJECT_RECV_BEGIN();
            break;
        case FaultScenario::RECV_SERVICE_START_ERROR:
            point = mlcd_inject_.INJECT_RECV_BEGIN();
            break;
        case FaultScenario::SEND_SERVICE_START_DELAY:
            point = mlcd_inject_.INJECT_SEND_BEGIN();
            is_remote = is_cluster_;
            break;
        case FaultScenario::SEND_SERVICE_START_ERROR:
            point = mlcd_inject_.INJECT_SEND_BEGIN();
            is_remote = is_cluster_;
            break;
        case FaultScenario::URMA_WRITE_DELAY:
            point = mlcd_inject_.INJECT_URMA_WRITE();
            is_remote = is_cluster_;
            break;
        case FaultScenario::URMA_WRITE_ERROR:
            point = mlcd_inject_.INJECT_URMA_WRITE();
            is_remote = is_cluster_;
            break;
        case FaultScenario::NOTIFY_CALLBACK_DELAY:
            point = mlcd_inject_.INJECT_NOTIFY_CALLBACK();
            break;
        case FaultScenario::NOTIFY_CALLBACK_ERROR:
            point = mlcd_inject_.INJECT_NOTIFY_CALLBACK();
            break;
        case FaultScenario::MLCD_FAULT_CHAIN:
            point = mlcd_inject_.INJECT_RECV_BEGIN();
            break;
        case FaultScenario::URMA_RECV_ERROR:
            point = mlcd_inject_.INJECT_URMA_RECV();
            break;
        case FaultScenario::SEND_FIRST_CHUNK_ERROR:
            point = mlcd_inject_.INJECT_SEND_FIRST_CHUNK();
            is_remote = is_cluster_;
            break;
        case FaultScenario::TASK_GROUP_ALLOC_ERROR:
            point = mlcd_inject_.INJECT_ALLOC_TASK_GROUP();
            break;
        default:
            return true;
        }
        std::string cmd;
        if (is_remote) {
            cmd = "ssh -o BatchMode=yes -o ConnectTimeout=5 " + remote_worker_ + " \"mlcd_inject_cli verify " + point
                  + " " + std::to_string(verify_seconds) + "\"";
        } else {
            cmd = "mlcd_inject_cli verify " + point + " " + std::to_string(verify_seconds);
        }
        int rc = system(cmd.c_str());
        return (rc == 0);
    }

    // 运行指定场景的故障测试
    bool RunFaultTest(FaultScenario scenario, int count = 10, int timeout_ms = 60000, int inject_delay_ms = -1)
    {
        std::cout << "\n========================================" << std::endl;
        std::cout << "Running Fault Test: " << g_scenarioDesc[scenario] << std::endl;
        std::cout << "========================================" << std::endl;
        if (inject_delay_ms >= 0) {
            std::cout << "[Config] Inject delay override: " << inject_delay_ms << "ms" << std::endl;
        }

        // 非 NO_FAULT 场景需要检查故障注入可用性
        if (scenario != FaultScenario::NO_FAULT && !IsFaultInjectionAvailable()) {
            std::cerr << "[Error] Fault injection not available. Please check libos_transport.so" << std::endl;
            return false;
        }

        // 设置故障场景
        if (!SetFaultScenario(scenario, inject_delay_ms)) {
            std::cerr << "[Warning] Some fault injection points failed to set" << std::endl;
            // 继续测试，因为部分注入可能已生效
        }

        // 准备数据
        GenerateData(count);

        // 提取所有 keys
        std::vector<std::string> keys;
        for (const auto &it : data_) {
            keys.push_back(it.first);
        }

        // 先写入数据
        std::cout << "Pre-populating data..." << std::endl;
        int writeFailed = 0;

        if (is_cluster_) {
            // 集群模式：强制向 remote worker 写入数据，确保 MGetH2D 必须走远程 pipeline
            std::cout << "[ClusterMode] Writing test data to remote worker: " << remote_worker_ << std::endl;
            ConnectOptions remoteOptions;
            remoteOptions.host = remote_worker_;
            remoteOptions.port = port_;
            remoteOptions.accessKey = "";
            remoteOptions.secretKey = "";
            remoteOptions.deviceId = std::to_string(gpu_id_);
            auto remoteClient = std::make_unique<KVClient>(remoteOptions);
            Status initRc = remoteClient->Init();
            if (initRc.IsError()) {
                std::cerr << "[Error] Failed to connect to remote worker for data prep: " << initRc.GetMsg()
                          << std::endl;
                ClearFaultScenario();
                return false;
            }
            for (const auto &it : data_) {
                Status rc = remoteClient->Set(it.first, it.second);
                if (rc.IsError()) {
                    std::cerr << "[Error] Set failed on remote worker: " << rc.GetMsg() << std::endl;
                    writeFailed++;
                }
            }
            if (writeFailed > 0) {
                std::cerr << "[Error] " << writeFailed << " keys failed to write on remote worker. Aborting test."
                          << std::endl;
                ClearFaultScenario();
                return false;
            }
        } else {
            // 单机模式：向本地写入
            for (const auto &it : data_) {
                Status rc = client_->Set(it.first, it.second);
                if (rc.IsError()) {
                    writeFailed++;
                }
            }
            if (writeFailed > 0) {
                std::cout << "Warning: " << writeFailed << " keys failed to write" << std::endl;
            }
        }

        // 准备测试
        std::vector<std::pair<void *, size_t>> devShmChunks;
        std::vector<std::string> outFailedKeys;

        cudaError_t err;
        if ((err = cudaSetDevice(gpu_id_)) != cudaSuccess) {
            std::cerr << "cudaSetDevice(" << gpu_id_ << ") failed: " << cudaGetErrorString(err) << std::endl;
            ClearFaultScenario();
            return false;
        }

        // 分配显存（使用 RAII 自动释放）
        bool alloc_failed = false;
        for (const auto &it : data_) {
            void *dev_ptr = nullptr;
            if ((err = cudaMalloc(&dev_ptr, it.second.size())) != cudaSuccess) {
                std::cerr << "cudaMalloc failed: " << cudaGetErrorString(err) << std::endl;
                alloc_failed = true;
                break;
            }
            devShmChunks.push_back({dev_ptr, it.second.size()});
        }

        // RAII 守卫，确保显存被释放
        CudaMemoryGuard cudaGuard(devShmChunks);

        if (alloc_failed) {
            ClearFaultScenario();
            return false;
        }

        // 执行测试
        FaultTestStats stats;
        stats.start_time = std::chrono::steady_clock::now();

        auto start = std::chrono::high_resolution_clock::now();
        Status ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys, static_cast<int32_t>(timeout_ms));
        auto end = std::chrono::high_resolution_clock::now();

        auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        stats.RecordLatency(latency);
        stats.total_requests++;

        std::cout << "[MGetH2DResult] ret=" << ret.ToString() << ", failedKeys=" << outFailedKeys.size()
                  << ", latency_ms=" << std::fixed << std::setprecision(3) << (static_cast<double>(latency) / 1000.0)
                  << ", timeout_ms=" << timeout_ms << std::endl;

        // 防伪验证：确认注入点确实触发了
        bool inject_verified = VerifyInjection(scenario, latency);
        if (!inject_verified) {
            std::cerr << "[CRITICAL] Injection point NOT triggered. "
                         "Test result is UNTRUSTWORTHY."
                      << std::endl;
            stats.failed_requests++;
            return false;
        }

        // 统计结果
        auto expected_it = g_expectedResult.find(scenario);
        ExpectedResult expected =
            (expected_it == g_expectedResult.end()) ? ExpectedResult::ANY_ERROR : expected_it->second;
        uint32_t expected_delay_ms = ExpectedDelayMsForScenario(scenario, inject_delay_ms);
        double latency_ms = static_cast<double>(latency) / 1000.0;
        bool is_pass = CheckResult(scenario, ret, outFailedKeys, latency, inject_delay_ms);
        if (is_pass) {
            stats.passed_requests++;
            std::cout << "[PASS] MGetH2D result matches expectation. " << "ret=" << ret.ToString()
                      << ", failedKeys=" << outFailedKeys.size() << ", expected=" << ExpectedResultToString(expected)
                      << ", latency_ms=" << std::fixed << std::setprecision(3) << latency_ms
                      << ", inject_delay_ms=" << expected_delay_ms << ", timeout_ms=" << timeout_ms << std::endl;
        } else {
            stats.failed_requests++;
            std::cerr << "[FAIL] Unexpected result. ret=" << ret.ToString() << ", failedKeys=" << outFailedKeys.size()
                      << ", expected=" << ExpectedResultToString(expected) << ", latency_ms=" << std::fixed
                      << std::setprecision(3) << latency_ms << ", inject_delay_ms=" << expected_delay_ms
                      << ", timeout_ms=" << timeout_ms << std::endl;
        }

        // 手动释放并禁用 RAII 自动释放
        for (auto &chunk : devShmChunks) {
            if (chunk.first) {
                cudaFree(chunk.first);
            }
        }
        cudaGuard.Release();

        // 清理测试数据
        if (delete_value_) {
            std::vector<std::string> failkeys;
            client_->Del(keys, failkeys);
            if (is_cluster_) {
                ConnectOptions remoteOptions;
                remoteOptions.host = remote_worker_;
                remoteOptions.port = port_;
                remoteOptions.accessKey = "";
                remoteOptions.secretKey = "";
                remoteOptions.deviceId = std::to_string(gpu_id_);
                auto remoteClient = std::make_unique<KVClient>(remoteOptions);
                if (remoteClient->Init().IsOk()) {
                    remoteClient->Del(keys, failkeys);
                }
            }
        }
        ClearFaultScenario();

        stats.PrintSummary(g_scenarioDesc[scenario]);
        return is_pass;
    }

    // 运行所有场景的故障测试
    bool RunAllFaultTests(int count = 10)
    {
        std::cout << "\n##################################################" << std::endl;
        std::cout << "#     Pipeline H2D Fault Test Suite (4/6/7/8)    #" << std::endl;
        std::cout << "##################################################" << std::endl;

        int passed = 0;
        int failed = 0;

        // 先运行正常流程作为基准
        if (RunFaultTest(FaultScenario::NO_FAULT, count)) {
            passed++;
        } else {
            failed++;
        }

        // 运行MLCacheDirect层的故障场景（流程4、6、7、8）
        if (RunFaultTest(FaultScenario::RECV_SERVICE_START_DELAY, count, 30000))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::RECV_SERVICE_START_ERROR, count))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::SEND_SERVICE_START_DELAY, count, 30000))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::SEND_SERVICE_START_ERROR, count))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::URMA_WRITE_DELAY, count, 30000))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::URMA_WRITE_ERROR, count))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::NOTIFY_CALLBACK_DELAY, count, 30000))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::NOTIFY_CALLBACK_ERROR, count))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::MLCD_FAULT_CHAIN, count, 30000))
            passed++;
        else
            failed++;

        // 补充场景（覆盖内部错误分支）
        if (RunFaultTest(FaultScenario::URMA_RECV_ERROR, count))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::SEND_FIRST_CHUNK_ERROR, count))
            passed++;
        else
            failed++;
        if (RunFaultTest(FaultScenario::TASK_GROUP_ALLOC_ERROR, count))
            passed++;
        else
            failed++;

        std::cout << "\n##################################################" << std::endl;
        std::cout << "#              All Tests Completed               #" << std::endl;
        std::cout << "#  Passed: " << passed << "  Failed: " << failed << "                       #" << std::endl;
        std::cout << "##################################################" << std::endl;

        return (failed == 0);
    }

private:
    void GenerateData(int count)
    {
        data_.clear();
        int actual_count = count;
        if (actual_count <= 0 && !custom_keys_.empty()) {
            actual_count = static_cast<int>(custom_keys_.size());
        }
        if (actual_count <= 0)
            actual_count = count_;
        data_.reserve(actual_count);

        // 预生成 pattern 数据，避免重复计算
        const size_t data_size = value_size_;
        std::string pattern;
        pattern.reserve(data_size);
        for (size_t j = 0; j < data_size; ++j) {
            pattern.push_back(static_cast<char>('A' + ((j + base_value_prefix_[0]) % 26)));
        }

        // 生成随机后缀，避免历史脏数据干扰
        auto now = std::chrono::steady_clock::now().time_since_epoch();
        uint64_t timestamp = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
        std::random_device rd;
        uint32_t randomSuffix = rd();

        for (int i = 0; i < actual_count; i++) {
            std::string value = pattern;
            if (value.size() > value_size_) {
                value = value.substr(0, value_size_);
            }

            std::string key;
            if (i < static_cast<int>(custom_keys_.size()) && !custom_keys_[i].empty()) {
                key = custom_keys_[i];
            } else {
                key = "fault_test_key_" + std::to_string(timestamp) + "_" + std::to_string(randomSuffix) + "_"
                      + std::to_string(i);
            }
            data_.emplace_back(std::move(key), std::move(value));
        }
        std::cout << "Generated " << actual_count << " test keys, value_size=" << value_size_ << std::endl;
    }

    std::string host_;
    int port_;
    int gpu_id_;
    std::string remote_worker_;
    std::unique_ptr<KVClient> client_;
    std::vector<std::pair<std::string, std::string>> data_;
    std::string base_value_prefix_ = "0";
    std::vector<std::string> custom_keys_;
    int count_ = 10;
    size_t value_size_ = 8388608;
    bool delete_value_ = true;
    MLCacheDirectInjectManager mlcd_inject_; // 本地注入（流程4/8）
    SSHRemoteInjector remote_inject_;        // 远程注入（流程6/7）
    bool is_cluster_;                        // 是否为集群模式
};

void PrintUsage(const char *prog)
{
    std::cout << "Usage: " << prog << " <host> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Arguments:" << std::endl;
    std::cout << "  host              Local dataserver (Get side, Worker1)" << std::endl;
    std::cout << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --port=N or --port N              : Server port (default: 18481)" << std::endl;
    std::cout << "  --remote-worker=IP or --remote-worker IP" << std::endl;
    std::cout << "                                    : Remote dataserver (Set side, Worker2)" << std::endl;
    std::cout << "  --scenario=N or --scenario N      : Run specific fault scenario (0-9,20-22)" << std::endl;
    std::cout << "  --all                             : Run all fault scenarios" << std::endl;
    std::cout << "  --count=N or --count N            : Number of keys to test (default: 10)" << std::endl;
    std::cout << "  --timeout=N or --timeout N        : Timeout in milliseconds (default: 60000)" << std::endl;
    std::cout << "  --inject_delay_ms=N or --inject_delay_ms N" << std::endl;
    std::cout << "                                    : Override sleep injection delay for a specific scenario"
              << std::endl;
    std::cout << "  --value_prefix=X or --value_prefix X" << std::endl;
    std::cout << "                                    : Base prefix for value (default: 0)" << std::endl;
    std::cout << "  --keys=k1,k2... or --keys k1,k2   : Comma-separated custom key list" << std::endl;
    std::cout << "  --value_size=N or --value_size N  : Length of generated value (default: 8388608)" << std::endl;
    std::cout
        << "  --thread=N or --thread N          : Accepted for pipeline_h2d compatibility; fault test runs serially"
        << std::endl;
    std::cout << "  --delete_value=true|false|1|0     : Delete keys after get (default: true)" << std::endl;
    std::cout << "  --gpu_id=N or --gpu_id N          : GPU device ID to use (default: 0)" << std::endl;
    std::cout << "  --list                            : List all fault scenarios" << std::endl;
    std::cout << "  --help or -h                      : Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Scenarios (MLCacheDirect Layer - Flow 4/6/7/8):" << std::endl;
    for (int i = 0; i <= 9; i++) {
        auto it = g_scenarioDesc.find(static_cast<FaultScenario>(i));
        if (it != g_scenarioDesc.end()) {
            std::cout << "  " << i << ": " << it->second << std::endl;
        }
    }
    for (int i = 20; i <= 22; i++) {
        auto it = g_scenarioDesc.find(static_cast<FaultScenario>(i));
        if (it != g_scenarioDesc.end()) {
            std::cout << "  " << i << ": " << it->second << std::endl;
        }
    }
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  Single-node (local):" << std::endl;
    std::cout << "    " << prog << " 127.0.0.1 --all" << std::endl;
    std::cout << std::endl;
    std::cout << "  Cluster (Worker1=local, Worker2=remote):" << std::endl;
    std::cout << "    " << prog << " 127.0.0.1 --remote-worker=10.0.0.2 --all" << std::endl;
    std::cout << "    " << prog << " 127.0.0.1 --remote-worker=10.0.0.2 --scenario=3" << std::endl;
    std::cout << "    " << prog << " 127.0.0.1 --remote-worker=10.0.0.2 --scenario=1 --inject_delay_ms=5000"
              << std::endl;
    std::cout << std::endl;
    std::cout << "Injection Points:" << std::endl;
    std::cout << "  Flow 4 (recv):    Local  (Get side) - recv.begin" << std::endl;
    std::cout << "  Flow 4 (recv):    Local  (Get side) - urma.recv" << std::endl;
    std::cout << "  Flow 6 (send):    Remote (Set side) - send.begin" << std::endl;
    std::cout << "  Flow 6 (send):    Remote (Set side) - send.first_chunk" << std::endl;
    std::cout << "  Flow 7 (urma):    Remote (Set side) - urma.write" << std::endl;
    std::cout << "  Flow 8 (notify):  Local  (Get side) - notify_callback" << std::endl;
    std::cout << "  Flow 4/6 (alloc): Both   sides      - task_group.alloc" << std::endl;
}

struct CmdArgs {
    std::string host;
    int port = 18481;
    std::string remote_worker;
    int scenario = -1;
    bool run_all = false;
    bool list_scenarios = false;
    int count = 10;
    int timeout = 60000;
    int inject_delay_ms = -1;
    std::string value_prefix = "0";
    std::vector<std::string> keys;
    size_t value_size = 8388608;
    int thread_count = 1;
    bool delete_value = true;
    std::string gpu_id_str = "0";
    int gpu_id = 0;
    bool help = false;
};

CmdArgs ParseArgs(int argc, char *argv[])
{
    CmdArgs args;
    if (argc < 2) {
        return args;
    }

    std::string first = argv[1];
    if (first == "--help" || first == "-h") {
        args.help = true;
        return args;
    }
    args.host = first;

    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        std::string key;
        std::string value;

        if (arg == "--help" || arg == "-h") {
            args.help = true;
            return args;
        }
        if (arg == "--all") {
            args.run_all = true;
            continue;
        }
        if (arg == "--list") {
            args.list_scenarios = true;
            continue;
        }

        if (ParseKeyValue(arg, key, value)) {
            if (key == "remote-worker")
                args.remote_worker = value;
            else if (key == "scenario")
                args.scenario = std::stoi(value);
            else if (key == "count")
                args.count = std::stoi(value);
            else if (key == "timeout")
                args.timeout = std::stoi(value);
            else if (key == "inject_delay_ms" || key == "inject-delay-ms")
                args.inject_delay_ms = std::stoi(value);
            else if (key == "value_prefix")
                args.value_prefix = value;
            else if (key == "keys")
                args.keys = SplitString(value, ',');
            else if (key == "value_size")
                args.value_size = std::stoull(value);
            else if (key == "thread")
                args.thread_count = std::stoi(value);
            else if (key == "delete_value")
                args.delete_value = ParseBool(value);
            else if (key == "gpu_id" || key == "gpu_num")
                args.gpu_id_str = value;
            else if (key == "port")
                args.port = std::stoi(value);
            continue;
        }

        if (arg.size() >= 3 && arg[0] == '-' && arg[1] == '-') {
            key = arg.substr(2);
            if (i + 1 < argc) {
                std::string next_arg = argv[i + 1];
                if (next_arg == "--help" || next_arg == "-h") {
                    args.help = true;
                    return args;
                }
            }

            if (i + 1 < argc && argv[i + 1][0] != '-') {
                value = argv[++i];
                if (key == "remote-worker")
                    args.remote_worker = value;
                else if (key == "scenario")
                    args.scenario = std::stoi(value);
                else if (key == "count")
                    args.count = std::stoi(value);
                else if (key == "timeout")
                    args.timeout = std::stoi(value);
                else if (key == "inject_delay_ms" || key == "inject-delay-ms")
                    args.inject_delay_ms = std::stoi(value);
                else if (key == "value_prefix")
                    args.value_prefix = value;
                else if (key == "keys")
                    args.keys = SplitString(value, ',');
                else if (key == "value_size")
                    args.value_size = std::stoull(value);
                else if (key == "thread")
                    args.thread_count = std::stoi(value);
                else if (key == "delete_value")
                    args.delete_value = ParseBool(value);
                else if (key == "gpu_id" || key == "gpu_num")
                    args.gpu_id_str = value;
                else if (key == "port")
                    args.port = std::stoi(value);
            }
        }
    }

    return args;
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        PrintUsage(argv[0]);
        return 1;
    }

    CmdArgs args = ParseArgs(argc, argv);

    if (args.help) {
        PrintUsage(argv[0]);
        return 0;
    }

    if (args.host.empty()) {
        PrintUsage(argv[0]);
        return 1;
    }

    try {
        args.gpu_id = std::stoi(args.gpu_id_str);
        if (args.gpu_id < 0) {
            std::cerr << "[WARN] Invalid gpu_id, defaulting to 0" << std::endl;
            args.gpu_id = 0;
        }
    } catch (...) {
        std::cerr << "[WARN] Failed to parse gpu_id, defaulting to 0" << std::endl;
        args.gpu_id = 0;
    }

    if (args.port <= 0 || args.port > 65535) {
        std::cerr << "[ERROR] Invalid port: " << args.port << ", must be 1-65535" << std::endl;
        return 1;
    }

    if (args.thread_count < 1) {
        std::cerr << "[ERROR] thread count must be >= 1" << std::endl;
        return 1;
    }
    if (args.thread_count > 1) {
        std::cerr << "[WARN] pipeline_h2d_fault_test accepts --thread for interface compatibility, "
                  << "but fault scenarios run serially" << std::endl;
    }
    if (args.inject_delay_ms < -1) {
        std::cerr << "[ERROR] inject_delay_ms must be >= 0" << std::endl;
        return 1;
    }
    if (args.run_all && args.inject_delay_ms >= 0) {
        std::cerr << "[WARN] --inject_delay_ms is ignored with --all; it only applies to a specific --scenario"
                  << std::endl;
    }

    if (args.list_scenarios) {
        std::cout << "Available Fault Scenarios (MLCacheDirect Layer):" << std::endl;
        for (int i = 0; i <= 9; i++) {
            auto it = g_scenarioDesc.find(static_cast<FaultScenario>(i));
            if (it != g_scenarioDesc.end()) {
                std::cout << "  " << i << ": " << it->second << std::endl;
            }
        }
        for (int i = 20; i <= 22; i++) {
            auto it = g_scenarioDesc.find(static_cast<FaultScenario>(i));
            if (it != g_scenarioDesc.end()) {
                std::cout << "  " << i << ": " << it->second << std::endl;
            }
        }
        return 0;
    }

    std::cout << "[Config] Local worker (Get): " << args.host << std::endl;
    std::cout << "[Config] Port: " << args.port << std::endl;
    std::cout << "[Config] GPU: " << args.gpu_id << std::endl;
    if (args.inject_delay_ms >= 0) {
        std::cout << "[Config] Inject delay override: " << args.inject_delay_ms << "ms" << std::endl;
    }
    if (!args.remote_worker.empty()) {
        std::cout << "[Config] Remote worker (Set): " << args.remote_worker << std::endl;
        std::cout << "[Config] Mode: Cluster (4 flows via SSH)" << std::endl;
    } else {
        std::cout << "[Config] Mode: Single-node" << std::endl;
    }

    // 验证场景ID是否有效
    auto IsValidScenario = [](int id) -> bool {
        switch (static_cast<FaultScenario>(id)) {
        case FaultScenario::NO_FAULT:
        case FaultScenario::RECV_SERVICE_START_DELAY:
        case FaultScenario::RECV_SERVICE_START_ERROR:
        case FaultScenario::SEND_SERVICE_START_DELAY:
        case FaultScenario::SEND_SERVICE_START_ERROR:
        case FaultScenario::URMA_WRITE_DELAY:
        case FaultScenario::URMA_WRITE_ERROR:
        case FaultScenario::NOTIFY_CALLBACK_DELAY:
        case FaultScenario::NOTIFY_CALLBACK_ERROR:
        case FaultScenario::MLCD_FAULT_CHAIN:
        case FaultScenario::URMA_RECV_ERROR:
        case FaultScenario::SEND_FIRST_CHUNK_ERROR:
        case FaultScenario::TASK_GROUP_ALLOC_ERROR:
            return true;
        default:
            return false;
        }
    };

    try {
        H2DFaultTest tester(args.host, args.remote_worker, args.port, args.gpu_id);
        tester.SetDataParams(args.value_prefix, args.keys, args.count, args.value_size, args.delete_value);

        // 检查故障注入可用性（如果需要）
        if (args.run_all || (args.scenario > 0 && IsValidScenario(args.scenario))) {
            if (!tester.IsFaultInjectionAvailable()) {
                std::cerr << "[Warning] Fault injection library not available. Only NO_FAULT test will run properly."
                          << std::endl;
            }
        }

        bool result = true;
        if (args.run_all) {
            result = tester.RunAllFaultTests(args.count);
        } else if (args.scenario >= 0 && IsValidScenario(args.scenario)) {
            result = tester.RunFaultTest(
                static_cast<FaultScenario>(args.scenario), args.count, args.timeout, args.inject_delay_ms);
        } else if (args.scenario < 0) {
            result = tester.RunFaultTest(FaultScenario::NO_FAULT, args.count, args.timeout);
        } else {
            std::cerr << "Error: Invalid scenario ID " << args.scenario << std::endl;
            PrintUsage(argv[0]);
            return 1;
        }

        return result ? 0 : 1;
    } catch (const std::exception &e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}

#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include "pipeline_rh2d_batch_data.h"
#include <iomanip>
#include <vector>
#include <sstream>
#include <string>
#include <fstream>
#include <iostream>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <algorithm>
#include <thread>
#include <mutex>
#include <memory>
#include <condition_variable>
#include <atomic>
#include <random>
#include <cmath>
#include <queue>
#include <deque>
#include <utility>

#ifdef USE_CUDA_MOCK
#include "cuda_mock.h"
#else
#include <cuda_runtime.h>
#endif

using namespace datasystem;

std::mutex print_mutex;
#define TLOG(t_idx, ...) do { \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cout << "[T" << t_idx << "] "; \
    std::cout << __VA_ARGS__ << std::endl; \
} while(0)

#define TERROR(...) do { \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cerr << "[ERROR] "; \
    std::cerr << __VA_ARGS__ << std::endl; \
} while(0)

#define TMAIN(...) do { \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cout << "[Main] "; \
    std::cout << __VA_ARGS__ << std::endl; \
} while(0)

#define TIMER_START(name) auto timer_start_##name = std::chrono::high_resolution_clock::now();

#define TIMER_END(t_idx, name, desc)                                                                                     \
    {                                                                                                                  \
        auto timer_end_##name = std::chrono::high_resolution_clock::now();                                             \
        auto duration =                                                                                                \
            std::chrono::duration_cast<std::chrono::microseconds>(timer_end_##name - timer_start_##name).count();      \
        TLOG(t_idx, desc << ": " << duration << " us");                                                                  \
    }



// ---------------------------------------------------------------------------
// General utilities
// ---------------------------------------------------------------------------
std::vector<std::string> SplitString(const std::string& str, char delimiter) {
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

// ---------------------------------------------------------------------------
// CUDA device helpers
// ---------------------------------------------------------------------------
#ifndef USE_CUDA_MOCK
static cudaError_t CudaCreateStream(cudaStream_t *stream)
{
    return cudaStreamCreateWithFlags(stream, cudaStreamNonBlocking);
}

static cudaError_t CudaWaitAndDestroyStream(cudaStream_t stream)
{
    if (stream == nullptr) {
        return cudaSuccess;
    }
    cudaError_t syncErr = cudaStreamSynchronize(stream);
    cudaError_t destroyErr = cudaStreamDestroy(stream);
    return syncErr != cudaSuccess ? syncErr : destroyErr;
}
#endif

static void CudaRegisterPinFuncs()
{
#ifndef USE_CUDA_MOCK
    CudaFuncs funcs;
    funcs.hostRegister = reinterpret_cast<HostRegisterFunc>(cudaHostRegister);
    funcs.hostUnregister = reinterpret_cast<HostUnregisterFunc>(cudaHostUnregister);
    funcs.getErrorString = reinterpret_cast<GetErrorStringFunc>(cudaGetErrorString);
    KVClient::RegisterCudaFuncs(funcs);
#endif
}

void CudaFreePtrs(std::vector<void*>& ptrs) {
    for (auto& ptr : ptrs) {
        if (ptr != nullptr) {
            cudaFree(ptr);
            ptr = nullptr;
        }
    }
    ptrs.clear();
}

void CudaFreeChunks(std::vector<Blob>& devShmChunks) {
    for (auto& chunk : devShmChunks) {
        if (chunk.pointer != nullptr) {
            cudaFree(chunk.pointer);
        }
    }
    devShmChunks.clear();
}

#ifndef USE_PIPLN_MOCK
int CudaSetDevice(int gpu_id, const std::string& prefix) {
    cudaError_t err = cudaSetDevice(gpu_id);
    if (err != cudaSuccess) {
        TERROR(prefix << " cudaSetDevice(" << gpu_id << ") failed before KVClient Init: "
               << cudaGetErrorString(err));
        return -1;
    }
    return 0;
}
#else
int CudaSetDevice(int, const std::string&) {
    return 0;
}
#endif

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------
struct LatencyPercentiles {
    double p50 = 0;
    double p90 = 0;
    double p95 = 0;
    double p99 = 0;
};

// Enhanced statistics structure with percentile tracking
// Core percentile math over an already-sorted vector.
double PercentileAt(const std::vector<double>& sorted, double percentile) {
    if (sorted.empty()) return 0;
    double index = (percentile / 100.0) * (sorted.size() - 1);
    size_t lower_idx = static_cast<size_t>(std::floor(index));
    size_t upper_idx = static_cast<size_t>(std::ceil(index));

    if (lower_idx == upper_idx || upper_idx >= sorted.size()) {
        return sorted[lower_idx];
    }

    double fraction = index - lower_idx;
    return sorted[lower_idx] * (1.0 - fraction) + sorted[upper_idx] * fraction;
}

// Computes P50/P90/P95/P99 from a single sort of the sample vector.
LatencyPercentiles ComputePercentiles(std::vector<double> latencies) {
    LatencyPercentiles result;
    if (latencies.empty()) return result;
    std::sort(latencies.begin(), latencies.end());
    result.p50 = PercentileAt(latencies, 50.0);
    result.p90 = PercentileAt(latencies, 90.0);
    result.p95 = PercentileAt(latencies, 95.0);
    result.p99 = PercentileAt(latencies, 99.0);
    return result;
}
struct LatencyStats {
    double total_time_us = 0;
    std::vector<double> latencies_us;

    void AddLatency(double latency_us) {
        latencies_us.push_back(latency_us);
        total_time_us += latency_us;
    }
};

// Unified per-operation statistics: latency samples plus op/failed counters. One type
// backs both the per-thread stats and the shared accumulator; the latency samples are
// appended under a mutex so concurrent workers can share a stats object safely.
struct OpStats {
    LatencyStats latency;
    std::atomic<int64_t> ops{0};
    std::atomic<int64_t> failed{0};
    mutable std::mutex mutex;

    // std::atomic/std::mutex disable implicit copies; snapshots are taken by value so
    // the per-thread stats can be returned/assigned while shared stats are only used
    // through a reference/pointer.
    OpStats() = default;
    OpStats(const OpStats& other)
        : latency(other.latency), ops(other.ops.load()), failed(other.failed.load()) {}
    OpStats(OpStats&& other)
        : latency(std::move(other.latency)), ops(other.ops.load()), failed(other.failed.load()) {}
    OpStats& operator=(const OpStats& other) {
        if (this != &other) {
            std::lock_guard<std::mutex> lock(mutex);
            std::lock_guard<std::mutex> other_lock(other.mutex);
            latency = other.latency;
            ops.store(other.ops.load());
            failed.store(other.failed.load());
        }
        return *this;
    }
    OpStats& operator=(OpStats&& other) {
        if (this != &other) {
            std::lock_guard<std::mutex> lock(mutex);
            std::lock_guard<std::mutex> other_lock(other.mutex);
            latency = std::move(other.latency);
            ops.store(other.ops.load());
            failed.store(other.failed.load());
        }
        return *this;
    }

    void AddLatency(double us) {
        std::lock_guard<std::mutex> lock(mutex);
        latency.AddLatency(us);
        if (latency.latencies_us.size() > 100000) {
            latency.latencies_us.erase(latency.latencies_us.begin());
        }
    }

    std::vector<double> GetLatencies() const {
        std::lock_guard<std::mutex> lock(mutex);
        return latency.latencies_us;
    }
};

// Unified per-run statistics: one OpStats entry per operation kind. The same type
// is used for the thread-local RunStats of the generic framework and for the shared kps
// accumulator, so the whole program reports through one per-op stats model.
struct RunStats {
    OpStats set;
    OpStats get;
    OpStats del;

    int64_t TotalOps() const { return set.ops.load() + get.ops.load() + del.ops.load(); }
    int64_t TotalFailed() const { return set.failed.load() + get.failed.load() + del.failed.load(); }

    // Prints a stats snapshot: per-op KPS/op counters and latency percentiles. The uniform
    // reporter uses this for both its per-second snapshots and the final one.
    void Print(double interval_s, const std::string& prefix = "") const {
        double total_ops = TotalOps();
        double actual_kps = interval_s > 0 ? total_ops / interval_s : 0;
        double set_kps = interval_s > 0 ? set.ops.load() / interval_s : 0;
        double get_kps = interval_s > 0 ? get.ops.load() / interval_s : 0;
        double del_kps = interval_s > 0 ? del.ops.load() / interval_s : 0;

        std::cout << std::fixed << std::setprecision(1);
        std::cout << prefix << "Actual KPS: " << actual_kps
                  << " (set: " << set_kps
                  << ", get: " << get_kps
                  << ", del: " << del_kps << ")" << std::endl;

        std::cout << prefix << "Total ops: " << static_cast<int64_t>(total_ops)
                  << " (set: " << set.ops.load()
                  << ", get: " << get.ops.load()
                  << ", del: " << del.ops.load()
                  << ", failed: " << TotalFailed() << ")" << std::endl;

        // Get latencies for percentile calculation
        std::vector<double> set_lats = set.GetLatencies();
        std::vector<double> get_lats = get.GetLatencies();
        std::vector<double> del_lats = del.GetLatencies();

        std::cout << std::fixed << std::setprecision(3);
        auto print_latency_line = [&](const char* label, const std::vector<double>& lats) {
            std::cout << prefix << label;
            if (lats.empty()) {
                std::cout << "N/A";
            } else {
                LatencyPercentiles p = ComputePercentiles(lats);
                std::cout << "avg=" << p.p50
                          << ", P90=" << p.p90
                          << ", P95=" << p.p95
                          << ", P99=" << p.p99;
            }
            std::cout << std::endl;
        };
        print_latency_line("Set latency (us): ", set_lats);
        print_latency_line("Get latency (us): ", get_lats);
        print_latency_line("Del latency (us): ", del_lats);
    }
};

// ---------------------------------------------------------------------------
// Command-line arguments
// ---------------------------------------------------------------------------
class CmdArgs {
public:
    void PrintUsage() const {
        std::cout << "Usage: " << prog_ << " <command> [options]" << std::endl;
        std::cout << std::endl;

        std::cout << "Commands:" << std::endl;
        std::cout << "  set       : set+<ip>" << std::endl;
        std::cout << "  originget : get+<ip> cudaMemcpy [del+<ip>]" << std::endl;
        std::cout << "  mgeth2d   : mgeth2d+<ip>        [del+<ip>]" << std::endl;
        std::cout << std::endl;

        std::cout << "  get       : set+<remoteip> get+<localip> cudaMemcpy del+<remoteip>" << std::endl;
        std::cout << "  rh2d      : set+<remoteip> mgeth2d+<localip>        del+<remoteip>" << std::endl;
        std::cout << std::endl;

        std::cout << "Options:" << std::endl;
        std::cout << "  --count=N           Total keys (default: 100)" << std::endl;
        std::cout << "  --batch=N           Keys per chunk for mgeth2d/rh2d/get (default: 1)" << std::endl;
        std::cout << "  --thread=N          Threads (default: 1)" << std::endl;
        std::cout << "  --duration=N        Run-time cap in seconds (0 = run until count is exhausted)" << std::endl;
        std::cout << "  --kps=N             Rate limit (keys per second; 0 = unlimited); combine with get/rh2d" << std::endl;
        std::cout << std::endl;

        std::cout << "  --value_size=CFG    Value size: single number or config (default: 8388608)" << std::endl;
        std::cout << "                      Config format: size1:num1,size2:num2,size3" << std::endl;
        std::cout << "                      Last size without :num = remaining" << std::endl;
        std::cout << "  --value_prefix=X    Base prefix for per-thread generated values (default: empty)" << std::endl;
        std::cout << "  --key_prefix=X      Prefix for generated keys (default: empty)" << std::endl;
        std::cout << std::endl;

        std::cout << "  --remoteip=IP       Worker IP used for Set/Del (required when the command has set/del ops)" << std::endl;
        std::cout << "  --localip=IP        Worker IP used for Get/MGetH2D client init (default: same as remoteip)" << std::endl;
        std::cout << "  --port=N            Server port (default: 18481)" << std::endl;
        std::cout << "  --gpu_id=N          GPU device ID (default: 0)" << std::endl;
        std::cout << std::endl;

        std::cout << "  --delete_value=Y/N  Delete the keys after mgeth2d/originget (default: Y)" << std::endl;
        std::cout << "  --verify=Y/N        Verify data (default: Y)" << std::endl;
        std::cout << "  --pin=Y/N           Register CUDA host-memory funcs before KVClient Init (default: Y)" << std::endl;
        std::cout << "  --use_user_stream=Y/N            Use new MGetH2D interface (default: N)" << std::endl;
        std::cout << "  --enable_local_cache=Y/N         Enable local cache (default: Y)" << std::endl;
        std::cout << "  --enable_client_direct_rh2d=Y/N  Enable client-direct RH2D (default: N)" << std::endl;
        std::cout << "  --client_direct_thread_num=N     Client-direct RH2D thread num (default: 32)" << std::endl;
        std::cout << "  --fast_transport_mem_size=N      Fast transport memory size (default: 2GB)" << std::endl;
        std::cout << "  --help, -h          Show this help" << std::endl;
        std::cout << std::endl;

        std::cout << "Examples:" << std::endl;
        std::cout << "  # single-host commands: the ip comes from --remoteip/--localip" << std::endl;
        std::cout << "  " << prog_ << " set --remoteip=141.61.91.188 --port=18581 --key_prefix k --count 40 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4" << std::endl;
        std::cout << "  " << prog_ << " mgeth2d --remoteip=141.61.91.189 --port=18581 --key_prefix k --count 40 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4" << std::endl;
        std::cout << std::endl;
        std::cout << "  # Batch test (4 threads, 1MB values)" << std::endl;
        std::cout << "  " << prog_ << " rh2d --count=400 --batch=10 --thread=4 --value_size=1048576 --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
        std::cout << std::endl;
        std::cout << "  # Mixed sizes (batch=10: 3x1KB + 2x4KB + 5x8KB)" << std::endl;
        std::cout << "  " << prog_ << " rh2d --count=100 --batch=10 --value_size=1024:3,4096:2,8192 --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
        std::cout << std::endl;
        std::cout << "  # New interface with user stream" << std::endl;
        std::cout << "  " << prog_ << " rh2d --count=100 --batch=10 --use_user_stream=Y --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
        std::cout << std::endl;
        std::cout << "  # Rate-limited get: 1000 ops/sec, 4 threads, up to 60 seconds" << std::endl;
        std::cout << "  " << prog_ << " get --count=400 --batch=10 --kps=1000 --thread=4 --duration=60 --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
    }

    // Prints the effective configuration for a run. Every flow that reports the parsed
    // arguments goes through this so the config output is consistent and every label
    // matches the corresponding --option name.
    void Print() const {
        TMAIN("Command: " << cmd);
        // --count / --batch / --thread
        TMAIN("count: " << count << ", batch: " << batch
              << ", thread: " << thread_count);
        // --kps / --duration
        if (kps > 0 || duration_s > 0) {
            TMAIN("kps: " << kps << ", duration: " << duration_s << "s");
        }
        // --value_size / --value_prefix / --key_prefix
        std::ostringstream oss;
        oss << "value_size: "
            << (value_size_config.empty() ? std::to_string(value_size) : value_size_config);
        if (!value_prefix.empty()) {
            oss << ", value_prefix: " << value_prefix;
        }
        if (!key_prefix.empty()) {
            oss << ", key_prefix: " << key_prefix;
        }
        TMAIN(oss.str());
        // --remoteip / --localip / --port / --gpu_id
        TMAIN("remoteip: " << remoteip << ", localip: " << localip
              << ", port: " << port << ", gpu_id: " << gpu_id);
        // --verify / --delete_value / --pin / --use_user_stream
        TMAIN("verify: " << (verify_data ? "Yes" : "No")
              << ", delete_value: " << (delete_value ? "Yes" : "No")
              << ", pin: " << (client_options.pin ? "Yes" : "No")
              << ", use_user_stream: " << (use_user_stream ? "Yes" : "No"));
        // --enable_local_cache / --enable_client_direct_rh2d / --client_direct_thread_num /
        // --fast_transport_mem_size
        TMAIN("enable_local_cache: " << (client_options.enable_local_cache ? "Yes" : "No")
              << ", enable_client_direct_rh2d: "
              << (client_options.enable_client_direct_rh2d ? "Yes" : "No")
              << ", client_direct_thread_num: " << client_options.client_direct_thread_num
              << ", fast_transport_mem_size: " << client_options.fast_transport_mem_size);
    }

    const char* prog_ = nullptr;   // program name (argv[0]); set by Parse(), used by usage/validation

    int count = 100;
    int batch = 1;
    int thread_count = 1;
    double kps = 0;  // Rate limit (keys per second; 0 = unlimited)
    int duration_s = 0;  // Run-time cap in seconds (0 = run until count is exhausted)
    std::string value_size_config;  // --value_size: a plain size or "size1:num1,size2:num2,size3"
    size_t value_size = 8388608;   // single size (plain --value_size, used by per-thread generation)

    std::string cmd;
    std::string remoteip;
    std::string localip;

    int port = 18481;
    int gpu_id = 0;
    bool verify_data = true;
    bool use_user_stream = false;
    bool delete_value = true;
    std::string value_prefix;       // empty by default (per-thread generation falls back to "0")
    std::string key_prefix;         // --key_prefix: prefix for generated keys (default: empty)
    bool help = false;

    struct ClientOptions {
        uint64_t fast_transport_mem_size = 2ULL * 1024 * 1024 * 1024;
        bool enable_local_cache = true;
        bool enable_client_direct_rh2d = false;
        int client_direct_thread_num = 32;
        bool pin = true;
    };
    ClientOptions client_options;

    void Parse(int argc, char* argv[]) {
        prog_ = argv[0];
        for (int i = 1; i < argc; i++) {
            std::string arg = argv[i];
            std::string key, value;

            if (arg == "--help" || arg == "-h") {
                help = true;
                return;
            }

            if (ParseKeyValue(arg, key, value)) {
                SetOption(key, value);
                continue;
            }

            if (arg.size() >= 3 && arg[0] == '-' && arg[1] == '-') {
                key = arg.substr(2);
                if (i + 1 < argc && argv[i + 1][0] != '-') {
                    value = argv[++i];
                    SetOption(key, value);
                }
            } else if (arg[0] != '-' && cmd.empty()) {
                // The only positional argument is the command.
                cmd = arg;
            }
        }
        return;
    }

    static bool IsKnownCommand(const std::string& c) {
        return c == "set" || c == "get" || c == "mgeth2d" || c == "originget" ||
               c == "rh2d";
    }

    // Validates the parsed command-specific arguments; fills `error` on failure. One rule
    // applies to every command: count must be a clean multiple of batch/thread so the
    // unified loop always takes whole batches. kps/duration are optional controls (rate
    // limit and time cap) and need no extra constraints.
    int Validate(std::string& error) const {
        if (count <= 0 || batch <= 0) {
            error = "count and batch must be positive integers";
            return -1;
        }
        if (thread_count <= 0) {
            error = "thread must be positive";
            return -1;
        }
        if (count % batch != 0) {
            error = "count must be divisible by batch";
            return -1;
        }
        if (count % thread_count != 0) {
            error = "count must be divisible by thread";
            return -1;
        }
        if (count % (batch * thread_count) != 0) {
            error = "count must be divisible by (batch * thread)";
            return -1;
        }
        return 0;
    }

    // Validates the parsed arguments and resolves the effective local/remote ips: if only
    // one of --remoteip/--localip is given the other mirrors it (a host-style positional ip
    // lands in localip). Prints the error (with usage when helpful) on failure.
    int ValidateAndResolve() {
        if (port <= 0 || port > 65535) {
            TERROR("Invalid port: " << port << ", must be 1-65535");
            return -1;
        }
        if (cmd.empty()) {
            TERROR("Command is required (set, get, mgeth2d, originget, or rh2d)");
            PrintUsage();
            return -1;
        }
        if (!IsKnownCommand(cmd)) {
            TERROR("Unknown command '" << cmd << "'");
            PrintUsage();
            return -1;
        }
        if (remoteip.empty() && localip.empty()) {
            TERROR("--remoteip or --localip is required");
            PrintUsage();
            return -1;
        }
        if (remoteip.empty()) {
            remoteip = localip;
        }
        if (localip.empty()) {
            localip = remoteip;
        }

        // One validation rule applies to every command (count/batch/thread divisibility).
        std::string error;
        if (Validate(error) != 0) {
            TERROR(error);
            return -1;
        }
        return 0;
    }

private:
    static bool ParseKeyValue(const std::string& arg, std::string& key, std::string& value) {
        if (arg.size() < 3 || arg[0] != '-' || arg[1] != '-') {
            return false;
        }
        size_t eq_pos = arg.find('=');
        if (eq_pos != std::string::npos) {
            key = arg.substr(2, eq_pos - 2);
            value = arg.substr(eq_pos + 1);
            return true;
        }
        return false;
    }

    static bool ParseBool(std::string value) {
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        return value == "y" || value == "yes" || value == "1" || value == "true";
    }

    static bool ParseBoolH2D(const std::string& str) {
        if (str == "true" || str == "1")
            return true;
        if (str == "false" || str == "0")
            return false;
        return true;
    }

    void SetOption(const std::string& key, const std::string& value) {
        if (key == "count") count = std::stoi(value);
        else if (key == "batch") batch = std::stoi(value);
        else if (key == "thread") thread_count = std::stoi(value);
        else if (key == "kps") kps = std::stod(value);
        else if (key == "duration") duration_s = std::stoi(value);
        else if (key == "value_size") {
            value_size_config = value;
            if (value.find(':') == std::string::npos && value.find(',') == std::string::npos) {
                value_size = std::stoull(value);  // plain number -> single size
            }
        }
        else if (key == "value_prefix") value_prefix = value;
        else if (key == "key_prefix") key_prefix = value;
        else if (key == "delete_value") delete_value = ParseBoolH2D(value);
        else if (key == "remoteip") remoteip = value;
        else if (key == "localip") localip = value;
        else if (key == "port") port = std::stoi(value);
        else if (key == "gpu_id" || key == "gpu_num") gpu_id = std::stoi(value);
        else if (key == "verify") verify_data = ParseBool(value);
        else if (key == "use_user_stream") use_user_stream = ParseBool(value);
        else if (key == "enable_local_cache") client_options.enable_local_cache = ParseBool(value);
        else if (key == "pin") client_options.pin = ParseBool(value);
        else if (key == "enable_client_direct_rh2d") {
            client_options.enable_client_direct_rh2d = ParseBool(value);
        }
        else if (key == "client_direct_thread_num") {
            client_options.client_direct_thread_num = std::stoi(value);
        }
        else if (key == "fast_transport_mem_size") {
            client_options.fast_transport_mem_size = std::stoull(value);
        }
    }
};

// Single parsed command-line configuration shared by every component of the run. It is a
// static instance so the executor, data source and clients all read the same parsed
// parameters directly instead of copying them through constructors.
static CmdArgs args;

// ---------------------------------------------------------------------------
// Test data
// ---------------------------------------------------------------------------
// Value-size configuration for a batch: parses "size1:num1,size2:num2,size3",
// validates it against the batch size, computes derived values and prepares the
// test data (loading from the on-disk cache or generating on demand).
class ValueSizeConfig {
public:
    ValueSizeConfig() = default;

    // Parses the value_size config string; throws std::invalid_argument on a
    // malformed entry. An empty string falls back to a single legacy entry of
    // `default_size` covering the remaining keys in the batch.
    ValueSizeConfig(const std::string& value_size_config, size_t default_size) {
        if (value_size_config.empty()) {
            configs_.push_back({default_size, -1});
            return;
        }
        std::vector<std::string> parts = SplitString(value_size_config, ',');
        for (const auto& part : parts) {
            std::vector<std::string> size_num = SplitString(part, ':');
            if (size_num.empty()) continue;

            size_t size = 0;
            try {
                size = std::stoull(size_num[0]);
            } catch (const std::exception&) {
                throw std::invalid_argument("invalid value_size '" + size_num[0] + "'");
            }
            int count = -1;  // -1 means remaining in batch
            if (size_num.size() > 1) {
                try {
                    count = std::stoi(size_num[1]);
                } catch (const std::exception&) {
                    throw std::invalid_argument("invalid value_size count '" + size_num[1] + "'");
                }
            }
            configs_.push_back({size, count});
        }
    }

    // Validates the parsed entries against the batch size; fills `error` on failure.
    int Validate(int batch, std::string& error) const {
        if (configs_.empty()) {
            error = "value_size config is empty";
            return -1;
        }

        bool has_remaining = false;
        int explicit_count = 0;
        for (const auto& config : configs_) {
            if (config.size == 0) {
                error = "value_size must be positive";
                return -1;
            }
            if (config.count == -1) {
                has_remaining = true;
                continue;
            }
            if (config.count <= 0) {
                error = "value_size item count must be positive or omitted for remaining";
                return -1;
            }
            explicit_count += config.count;
        }

        if (!has_remaining && explicit_count < batch) {
            error = "explicit value_size item count is smaller than batch and no remaining size is configured";
            return -1;
        }
        return 0;
    }

    size_t max_size() const {
        size_t max_size = 0;
        for (const auto& config : configs_) {
            max_size = std::max(max_size, config.size);
        }
        return max_size;
    }

    const std::vector<rh2d_batch_data::SizeConfig>& configs() const { return configs_; }

    // Extends the explicit per-size counts to cover a full batch when no "remaining" size is
    // configured: distributes the shortfall across every configured size until their counts
    // sum to `batch`. Runs during argument parsing so the batch template always covers all
    // batch slots.
    void EnsureBatchCoverage(int batch) {
        bool has_remaining = false;
        int explicit_count = 0;
        for (const auto& config : configs_) {
            if (config.count == -1) {
                has_remaining = true;
                break;
            }
            explicit_count += config.count;
        }
        if (has_remaining || explicit_count >= batch || configs_.empty()) {
            return;
        }
        int shortfall = batch - explicit_count;
        int idx = 0;
        while (shortfall > 0) {
            configs_[idx].count++;
            shortfall--;
            idx = (idx + 1) % static_cast<int>(configs_.size());
        }
    }

    void Print() const {
        std::ostringstream oss;
        oss << "value_size: ";
        for (size_t i = 0; i < configs_.size(); ++i) {
            if (i > 0) oss << ", ";
            oss << configs_[i].size << " bytes";
            if (configs_[i].count > 0) {
                oss << " x " << configs_[i].count;
            } else {
                oss << " (remaining)";
            }
        }
        TMAIN(oss.str());
    }

private:
    std::vector<rh2d_batch_data::SizeConfig> configs_;
};
// Unified data source for every command. Data is deterministic (per-thread keys
// `{key_prefix}i_T{t_idx}`, values filled from --value_prefix) and generated lazily as a
// single batch template per worker thread by ThreadSlice(t_idx). The reserved round prefix
// (kRoundLen bytes) is stamped by the executor each round, so the batch is generated once
// instead of once per round.
class DataProvider {
public:
    // Parses the value_size config (single size or mixed sizes), validates it against the
    // batch, and stores the per-thread key setup. `--count` is the total number of keys
    // across all threads; each thread handles count/thread_count of them.
    static std::shared_ptr<DataProvider> Create(std::string& error) {
        ValueSizeConfig size_configs;
        try {
            size_configs = ValueSizeConfig(args.value_size_config, args.value_size);
        } catch (const std::exception& e) {
            error = e.what();
            return nullptr;
        }
        std::string value_size_error;
        if (size_configs.Validate(args.batch, value_size_error) != 0) {
            error = value_size_error;
            return nullptr;
        }
        // Extend the explicit per-size counts to cover the whole batch (argument-parsing stage).
        size_configs.EnsureBatchCoverage(args.batch);
        auto provider = std::shared_ptr<DataProvider>(new DataProvider());
        provider->size_configs_ = size_configs;
        provider->value_prefix_ = args.value_prefix;
        provider->key_prefix_ = args.key_prefix;
        return provider;
    }

    // Prints "[Main] value_size: ..." from the parsed config.
    void PrintSizeConfig() const {
        size_configs_.Print();
    }

    // Generates (or loads from the per-thread disk cache) the single batch template for
    // worker thread t_idx. The disk cache is skipped when --key_prefix is given (the data is
    // deterministic by construction); otherwise the batch template is cached per thread.
    rh2d_batch_data::Data ThreadSlice(int t_idx) {
        return GenerateOrLoadThread(t_idx);
    }

    // Reserved length of the round prefix stamped into each key/value by the executor.
    static constexpr size_t kRoundLen = 8;

private:
    DataProvider() = default;

    // Loads the thread's single batch template from its per-thread disk cache, or generates
    // it deterministically and caches it under the per-thread signature. The cache is used
    // for every invocation so separate commands (e.g. set then mgeth2d) share the same
    // generated data and verify byte-for-byte.
    rh2d_batch_data::Data GenerateOrLoadThread(int t_idx) {
        // Per-thread cache signature: the batch signature plus the thread index and key
        // prefix, so each worker thread's batch template is cached separately.
        std::ostringstream sig;
        sig << rh2d_batch_data::BuildSignature(args.batch, args.batch, size_configs_.configs())
            << ";t_idx=" << t_idx << ";kp=" << key_prefix_;
        std::string signature = sig.str();
        std::string cache_path = rh2d_batch_data::BuildCachePath(signature);
        rh2d_batch_data::Data slice;
        std::string error;
        if (rh2d_batch_data::Load(cache_path, signature, args.batch, size_configs_.max_size() + kRoundLen, slice, error)) {
            TLOG(t_idx, "load data ok, " << cache_path);
            return slice;
        }

        slice = GenerateThread(t_idx);

        std::string save_error;
        if (rh2d_batch_data::Save(cache_path, signature, slice, save_error)) {
            TMAIN("Test data cached: " << cache_path);
        } else {
            TMAIN("failed to cache test data: " << save_error);
        }
        return slice;
    }

    // Builds the thread's single batch template by delegating to rh2d_batch_data::Generate:
    // `round_len` placeholder + key_prefix + i + "_T" + t_idx, values the
    // "##########|segment|random16..." pattern from the value_size config. The reserved
    // round prefix is stamped by the executor each round. Reports generation progress.
    rh2d_batch_data::Data GenerateThread(int t_idx) const {
        int progressInterval = std::max(1, args.batch / 5);
        rh2d_batch_data::Data slice;
        rh2d_batch_data::Generate(
            args.batch, args.batch, size_configs_.configs(), key_prefix_, value_prefix_, t_idx, kRoundLen, slice,
            [&](size_t generated) {
                if (static_cast<int>(generated) % progressInterval == 0 || generated == static_cast<size_t>(args.batch)) {
                    TLOG(t_idx, "Generate progress: " << generated * 100 / args.batch
                         << "% (" << generated << "/" << args.batch << ")");
                }
            }
        );
        TLOG(t_idx, "generate data ok, batch=" << args.batch
             << ", value_size=" << size_configs_.max_size());
        return slice;
    }

    ValueSizeConfig size_configs_;
    std::string value_prefix_;
    std::string key_prefix_;
};

// ---------------------------------------------------------------------------
// Synchronization & threading
// ---------------------------------------------------------------------------
class Barrier {
public:
    Barrier(int count) : total_(count), count_(count), generation_(0), stop_(false)
    {}

    bool Wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        int gen = generation_;
        if (--count_ == 0) {
            generation_++;
            count_ = total_;
            cv_.notify_all();
        } else if (!stop_) {
            cv_.wait(lock, [this, gen] { return stop_ || gen != generation_; });
        }
        return stop_;
    }
    void Stop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (stop_) {
            return;
        }
        stop_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int total_;
    int count_;
    int generation_;
    bool stop_;
};
// Token bucket rate limiter
class RateLimiter {
public:
    RateLimiter(double rate, int burst_size = 100)
        : rate_(rate), tokens_(burst_size), max_tokens_(burst_size),
          last_time_(std::chrono::steady_clock::now()) {}

    void Acquire(int n = 1) {
        if (n <= 0) {
            return;
        }
        std::unique_lock<std::mutex> lock(mutex_);
        max_tokens_ = std::max(max_tokens_, static_cast<double>(n));
        while (true) {
            if (rate_ <= 0) {
                return;
            }
            RefillTokens();
            if (tokens_ >= n) {
                tokens_ -= n;
                return;
            }
            double deficit = n - tokens_;
            auto wait_us = static_cast<int64_t>(std::ceil(deficit / rate_ * 1000000.0));
            lock.unlock();
            std::this_thread::sleep_for(std::chrono::microseconds(std::max<int64_t>(1, wait_us)));
            lock.lock();
        }
    }

    void SetRate(double rate) {
        std::lock_guard<std::mutex> lock(mutex_);
        RefillTokens();
        rate_ = rate;
    }

private:
    void RefillTokens() {
        auto now = std::chrono::steady_clock::now();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - last_time_).count();
        last_time_ = now;
        double new_tokens = (elapsed_us / 1000000.0) * rate_;
        tokens_ = std::min(max_tokens_, tokens_ + new_tokens);
    }

    double rate_;  // tokens per second
    double tokens_;
    double max_tokens_;
    std::chrono::steady_clock::time_point last_time_;
    std::mutex mutex_;
};

// ---------------------------------------------------------------------------
// Operation primitives
// ---------------------------------------------------------------------------
// Unified operation primitives shared by every command family. Each primitive performs
// one KV operation over a batch slice, records its latency/counters into the matching
// per-op stats, verifies the data when enabled, and manages CUDA buffers (reusing the
// pre-allocated pool when verification is disabled). It owns the shared KVClient, the
// per-thread data slice and the CUDA device context used by every command family.
class OpPrimitives {
public:
    OpPrimitives(std::shared_ptr<KVClient> client, int t_idx)
        : client_(std::move(client)), t_idx_(t_idx), gpu_id_(args.gpu_id),
          verify_data_(args.verify_data),
          use_user_stream_(args.use_user_stream), user_stream_requested_(args.use_user_stream),
          silent_success_(args.kps > 0 || args.duration_s > 0) {
        if (client_ == nullptr) {
            throw std::runtime_error("shared KVClient is nullptr");
        }
#ifdef USE_CUDA_MOCK
        if (user_stream_requested_) {
            TLOG(t_idx_, "[WARN] use_user_stream is not supported in mock mode, falling back to old interface");
        }
        use_user_stream_ = false;
#endif
    }

    ~OpPrimitives() { Cleanup(); }

    // The verification helpers compare against the thread's full data slice.
    void SetSharedData(const rh2d_batch_data::Data& data) {
        data_ = data;
    }

    // One-time device setup and (optionally) user-stream creation. Returns false on failure.
    int Init() {
        cudaError_t err;
        if ((err = cudaSetDevice(gpu_id_)) != cudaSuccess) {
            TLOG(t_idx_, "cudaSetDevice(" << gpu_id_ << ") failed: " << cudaGetErrorString(err));
            return -1;
        }
        // Size the pre-allocated buffer pool from the thread's data slice (one extra byte for
        // the Get+H2D terminator, mirroring the batch flow's max(size+1) pre-allocation).
        max_value_size_ = 0;
        for (const auto& kv : data_) {
            max_value_size_ = std::max(max_value_size_, kv.second.size());
        }
#ifndef USE_CUDA_MOCK
        if (use_user_stream_) {
            if ((err = CudaCreateStream(&h2dStream_)) != cudaSuccess) {
                TLOG(t_idx_, "cudaStreamCreateWithFlags failed: " << cudaGetErrorString(err));
                return -1;
            }
            TLOG(t_idx_, "Using user-provided CUDA stream for H2D operations");
        }
#endif
        return 0;
    }

    // Set: writes every key of the batch, timing the whole batch as one latency sample.
    // Returns false when any key failed (the kps loop then skips the Get/Del of this
    // iteration); the set command additionally prints the key list.
    int Set(KVClient& client, const rh2d_batch_data::Data& batch,
             RunStats& stats, int round, bool emit_keys = false) {
        auto start = std::chrono::high_resolution_clock::now();
        int failed = 0;
        for (const auto& kv : batch) {
            Status rc = client.Set(kv.first, kv.second);
            if (rc.IsError()) {
                failed++;
                TLOG(t_idx_, rc.GetMsg());
            }
        }
        auto end = std::chrono::high_resolution_clock::now();
        double us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        stats.set.AddLatency(us);
        stats.set.ops += static_cast<int64_t>(batch.size());
        stats.set.failed += static_cast<int64_t>(failed);

        if (emit_keys) {
            std::ostringstream oss;
            std::string rlabel = round >= 0 ? ("Round " + std::to_string(round) + " ") : "";
            oss << rlabel << "Set keys " << batch.size() - failed << "/" << batch.size() << ": ";
            const size_t print_num = 5;
            for (size_t i = 0; i < batch.size(); ++i) {
                if (i + 1 > print_num) {
                    oss << ", ... (" << (batch.size() - print_num) << " more)";
                    break;
                }
                if (i > 0)
                    oss << ", ";
                oss << batch[i].first;
            }
            TLOG(t_idx_, oss.str());
        }
        return failed == 0 ? 0 : -1;
    }

    // MGetH2D: single batched H2D read into (reused or per-batch) CUDA buffers, timing the
    // whole batch as one latency sample. The pre-allocated pool is used when verification is
    // disabled; per-batch buffers are allocated (and freed) when verifying. `round` and
    // `verify_start` keep the per-round logging/verification indices. `use_user_stream` selects
    // the new MGetH2D interface with an external CUDA stream. Returns false on a fatal device
    // failure (caller stops the run).
    int MGetH2D(KVClient& client, const rh2d_batch_data::Data& batch,
                RunStats& stats, int round, int verify_start = 0) {
        cudaError_t err;
        std::vector<std::string> keys;
        std::vector<Blob> devShmChunks;
        std::vector<std::string> outFailedKeys;
        for (const auto& kv : batch) {
            keys.push_back(kv.first);
        }
        std::string rlabel = round >= 0 ? ("Round " + std::to_string(round) + " ") : "";
        const char* opname = "MGetH2D";

        if (AllocateDestBuffers(batch, devShmChunks, stats) != 0) {
            return -1;
        }

        std::vector<Optional<ReadOnlyBuffer>> readOnlyBuffers;  // kept alive by the user stream
        auto start_time = std::chrono::high_resolution_clock::now();
        double sync_us = 0;  // cudaStreamSynchronize wait time (user-stream interface)
        Status ret;
#ifndef USE_CUDA_MOCK
        if (use_user_stream_) {
            ret = client.MGetH2D(keys, devShmChunks, outFailedKeys,
                                 reinterpret_cast<void*>(h2dStream_), &readOnlyBuffers);
            auto sync_start = std::chrono::high_resolution_clock::now();
            err = cudaStreamSynchronize(h2dStream_);
            sync_us = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::high_resolution_clock::now() - sync_start).count();
            if (err != cudaSuccess) {
                TLOG(t_idx_, "cudaStreamSynchronize failed: " << cudaGetErrorString(err));
                ret = Status(StatusCode::K_RUNTIME_ERROR, cudaGetErrorString(err));
            }
        } else {
            ret = client.MGetH2D(keys, devShmChunks, outFailedKeys);
        }
#else
        ret = client.MGetH2D(keys, devShmChunks, outFailedKeys);
#endif
        auto end_time = std::chrono::high_resolution_clock::now();
        double duration_us =
            std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();

        bool op_failed = ret.IsError() || !outFailedKeys.empty();
        if (!op_failed) {
            stats.get.AddLatency(duration_us);
            stats.get.ops += static_cast<int64_t>(batch.size());
            if (!silent_success_) {
                TLOG(t_idx_, rlabel << opname << ": " << duration_us - sync_us
                     << " us, Sync: " << sync_us << " us, total: " << duration_us << " us");
            }
        } else {
            TLOG(t_idx_, rlabel << opname << " failed. Status: " << ret.GetMsg()
                 << ", Failed keys count: " << outFailedKeys.size());
            if (!outFailedKeys.empty()) {
                TLOG(t_idx_, "Failed keys:");
                for (const auto& key : outFailedKeys) {
                    TLOG(t_idx_, "  " << key);
                }
            }
            stats.get.failed += static_cast<int64_t>(outFailedKeys.empty() ? batch.size() : outFailedKeys.size());
        }

        if (verify_data_ && !op_failed) {
            CheckDataBatch(verify_start, devShmChunks);
        }
        // Buffers come from the shared pre-allocated pool (both verification modes); the pool
        // is freed once by Cleanup() and reused across rounds.
        return 0;
    }

    // GetH2D: Get into host buffers then cudaMemcpy them to CUDA buffers, recording the
    // get/h2d latency breakdown. The pre-allocated single buffer is used when verification is
    // disabled; per-batch buffers are allocated (and freed) when verifying. `round` and
    // `verify_start` keep the per-round logging/verification indices. Returns false on a fatal
    // device failure (caller stops the run).
    int GetH2D(KVClient& client, const rh2d_batch_data::Data& batch,
                RunStats& stats, int round, int verify_start = 0) {
        cudaError_t err;
        std::vector<std::string> keys;
        std::vector<Blob> devShmChunks;
        std::vector<Optional<ReadOnlyBuffer>> readOnlyBuffers;
        for (const auto& kv : batch) {
            keys.push_back(kv.first);
        }
        std::string rlabel = round >= 0 ? ("Round " + std::to_string(round) + " ") : "";
        const char* opname = "Get";

        if (AllocateDestBuffers(batch, devShmChunks, stats) != 0) {
            return -1;
        }

        auto get_start = std::chrono::high_resolution_clock::now();
        Status rc = client.Get(keys, readOnlyBuffers, 6000000);
        auto get_end = std::chrono::high_resolution_clock::now();
        double get_us = std::chrono::duration_cast<std::chrono::microseconds>(get_end - get_start).count();

        if (rc.IsError()) {
            TLOG(t_idx_, rlabel << opname << " failed. Status: " << rc.GetMsg()
                 << ", Failed keys count: " << batch.size());
            stats.get.failed += static_cast<int64_t>(batch.size());
            return 0;
        }

        auto h2d_start = std::chrono::high_resolution_clock::now();
        for (size_t i = 0; i < keys.size(); ++i) {
            if (i >= readOnlyBuffers.size() || !readOnlyBuffers[i]) {
                TLOG(t_idx_, "Get returned empty buffer for key " << i);
                continue;
            }
            if ((err = cudaMemcpy(devShmChunks[i].pointer, readOnlyBuffers[i]->ImmutableData(),
                    readOnlyBuffers[i]->GetSize(), cudaMemcpyHostToDevice)) != cudaSuccess) {
                TLOG(t_idx_, "cudaMemcpy failed for key " << i << ": " << cudaGetErrorString(err));
            }
        }
        auto h2d_end = std::chrono::high_resolution_clock::now();
        double h2d_us = std::chrono::duration_cast<std::chrono::microseconds>(h2d_end - h2d_start).count();
        double total_us = get_us + h2d_us;

        stats.get.AddLatency(total_us);
        stats.get.ops += static_cast<int64_t>(batch.size());

        if (!silent_success_) {
            TLOG(t_idx_, rlabel << opname << ": " << get_us
                 << " us, H2D: " << h2d_us << " us, total: " << total_us << " us");
        }

        if (verify_data_) {
            // GetH2D already copied the host buffers into the device buffers, so the unified
            // device verification applies the same way as MGetH2D.
            CheckDataBatch(verify_start, devShmChunks);
        }
        // Buffers come from the shared pre-allocated pool (both verification modes); the pool
        // is freed once by Cleanup() and reused across rounds.
        return 0;
    }

    // Del: deletes the whole batch in one batched KVClient::Del call, timing it as one
    // latency sample.
    int Del(KVClient& client, const rh2d_batch_data::Data& batch,
             RunStats& stats) {
        std::vector<std::string> keys;
        for (const auto& kv : batch) {
            keys.push_back(kv.first);
        }
        std::vector<std::string> failkeys;
        auto start = std::chrono::high_resolution_clock::now();
        Status rc = client.Del(keys, failkeys);
        auto end = std::chrono::high_resolution_clock::now();
        double us = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
        stats.del.AddLatency(us);
        stats.del.ops += static_cast<int64_t>(batch.size());
        if (rc.IsError()) {
            stats.del.failed += static_cast<int64_t>(batch.size());
            TLOG(t_idx_, "del failed " << rc.GetMsg());
        }
        return 0;
    }

private:

    // Allocates the destination CUDA buffers for a batch read (MGetH2D or Get+H2D). Both
    // verification modes share the pre-allocated pool: one buffer per key, sized
    // max_value_size_+1, reused across rounds and freed once by Cleanup(). out[i].size
    // records the actual value size so the SDK write and the device verification use the
    // exact byte count. Returns 0 on success, -1 on a fatal allocation failure (stats.get.failed
    // is updated).
    int AllocateDestBuffers(const rh2d_batch_data::Data& batch, std::vector<Blob>& out,
                            RunStats& stats) {
        if (EnsurePreallocated(static_cast<int>(batch.size()), max_value_size_ + 1) != 0) {
            stats.get.failed += static_cast<int64_t>(batch.size());
            return -1;
        }
        for (int i = 0; i < static_cast<int>(batch.size()); ++i) {
            out.push_back(
                Blob{preallocated_[i].pointer, static_cast<uint64_t>(batch[i].second.size())});
        }
        return 0;
    }

    // Copies the received device buffers back to host and compares them against the
    // thread's expected data slice (MGetH2D path).
    void CheckDataBatch(int startIdx, const std::vector<Blob>& devShmChunks) {
#ifdef USE_CUDA_MOCK
        TLOG(t_idx_, "CheckDataBatch skipped in mock mode (no real GPU memory to verify)");
#else
        bool is_failed = false;
        for (size_t i = 0; i < devShmChunks.size(); i++) {
            void* ptr = devShmChunks[i].pointer;
            size_t size = devShmChunks[i].size;
            auto& expected = data_[startIdx + i].second;
            std::string readOutData;
            cudaError_t err;

            readOutData.resize(size);
            if ((err = cudaMemcpy(reinterpret_cast<void*>(readOutData.data()), ptr, size, cudaMemcpyDeviceToHost)) != cudaSuccess) {
                TLOG(t_idx_, "cudaMemcpy D2H failed for " << (startIdx + i) << "th: " << cudaGetErrorString(err));
                is_failed = true;
                continue;
            }
            if (readOutData != expected) {
                TLOG(t_idx_, "Data mismatch at index " << (startIdx + i));
                is_failed = true;
            }
        }
        if (is_failed) {
            TLOG(t_idx_, "CheckDataBatch failed!");
        } else if (!silent_success_) {
            TLOG(t_idx_, "CheckDataBatch success!");
        }
#endif
    }

    // Pre-allocates `buffer_count` CUDA buffers of `buffer_size` bytes each, stored in the
    // shared preallocated_ pool. Reused across rounds once allocated; a failed allocation
    // frees the partial pool and returns -1.
    int EnsurePreallocated(int buffer_count, size_t buffer_size) {
        if (!preallocated_.empty()) {
            return 0;
        }
        cudaError_t err;
        for (int i = 0; i < buffer_count; ++i) {
            void* dev_ptr = nullptr;
            if ((err = cudaMalloc(&dev_ptr, buffer_size)) != cudaSuccess) {
                TLOG(t_idx_, "cudaMalloc failed for pre-allocated buffer slot " << i
                     << ": " << cudaGetErrorString(err));
                for (auto& buf : preallocated_) {
                    cudaFree(buf.pointer);
                }
                preallocated_.clear();
                return -1;
            }
            preallocated_.push_back(Blob{dev_ptr, static_cast<uint64_t>(buffer_size)});
        }
        TLOG(t_idx_, "cudaMalloc size " << buffer_size 
             << " x count " << preallocated_.size()
             << " = " << buffer_size * preallocated_.size()
             << " byte");
        return 0;
    }

    void Cleanup() {
        for (auto& buf : preallocated_) {
            if (buf.pointer) {
                cudaFree(buf.pointer);
            }
        }
        preallocated_.clear();
#ifndef USE_CUDA_MOCK
        if (h2dStream_ != nullptr) {
            CudaWaitAndDestroyStream(h2dStream_);
            h2dStream_ = nullptr;
        }
#endif
    }

    std::shared_ptr<KVClient> client_;
    rh2d_batch_data::Data data_;
    int t_idx_ = 0;
    int gpu_id_ = 0;
    bool verify_data_ = true;
    bool use_user_stream_ = false;
    bool user_stream_requested_ = false;
    bool silent_success_ = false;
    size_t max_value_size_ = 0;
    cudaStream_t h2dStream_ = nullptr;
    std::vector<Blob> preallocated_;
};

// ---------------------------------------------------------------------------
// Generic execution framework
// ---------------------------------------------------------------------------
// A Workload describes which KV operations a command runs (and on which client role) and
// when they run (once/loop/cleanup). One Executor drives a thread through the workload
// with a single unified loop using the shared OpPrimitives primitives, reading the run
// controls (batch/kps/duration) directly from the shared `args`, so every command shares
// the orchestration, data slicing and per-op stats.

enum class ClientRole { Local, Remote };

enum class OpKind { Set, MGetH2D, GetH2D, Del };

// One step of a workload. sync_before/sync_after place a barrier around the op so the
// multi-stage barrier layouts of the h2d commands are preserved.
struct Op {
    OpKind kind = OpKind::MGetH2D;
    ClientRole role = ClientRole::Local;
    bool sync_before = false;
    bool sync_after = false;
    bool emit_keys = false;  // Set: print the key list (set command)
};

// A command is just a combination of ops (once/loop/cleanup). The run controls (batch,
// rate limit, duration, verification, user stream) are read directly from the shared
// `args` instance by the executor; the workload only carries the op plan and the
// command-specific flags (barrier behaviour).
struct Workload {
    std::vector<Op> once_ops;    // executed once per thread before the loop
    std::vector<Op> loop_ops;    // executed on every batch slice
    std::vector<Op> cleanup_ops; // executed once per thread after the loop
    bool stop_barrier_on_error = false;  // stop the shared barrier on a fatal error
};

// The clients one run works on. For batch commands Local = shared client and Remote =
// the (possibly distinct) set/del client; for h2d commands both roles resolve to the
// single host client.
struct ClientContext {
    std::shared_ptr<KVClient> localClient;
    std::shared_ptr<KVClient> remoteClient;
    KVClient* Resolve(ClientRole role) const {
        return role == ClientRole::Remote ? remoteClient.get() : localClient.get();
    }
};

// Timing/state shared across the threads of one run. The shared per-op stats feed the
// uniform stats reporter (one snapshot per second plus a final one); timed workloads also
// use the rate limiter and the running gate.
struct RunControl {
    std::shared_ptr<RateLimiter> rate_limiter;
    std::atomic<bool> running{false};
    std::atomic<bool> stop{false};
    std::shared_ptr<RunStats> stats;  // shared per-op stats accumulated by all threads
};

// Everything a worker thread needs to describe its run: the workload (ops + execution
// mode), the clients it operates on, and the shared control state (kps timing/stats).
// Bundling them keeps the Executor construction short; each field keeps its own
// responsibility (workload = declarative config, clients = runtime resources).
struct RunPlan {
    Workload workload;
    std::shared_ptr<ClientContext> clients;
    std::shared_ptr<RunControl> control;
};

// Drives one thread through a Workload: once/loop/cleanup phases with per-op barriers,
// and the single unified loop (the run length and rate limit come from the shared `args`
// count/batch/kps/duration). Every iteration mirrors its per-op deltas into the shared
// RunStats used by the uniform reporter.
class Executor {
public:
    Executor(int t_idx, const std::shared_ptr<Barrier>& barrier, const RunPlan& plan,
             rh2d_batch_data::Data data)
        : t_idx_(t_idx), barrier_(barrier), context_(plan.clients), workload_(plan.workload),
          control_(plan.control), data_(std::move(data)) {}

    RunStats Run() {
        RunStats stats;
        if (!context_ || context_->localClient == nullptr) {
            return stats;
        }
        TLOG(t_idx_, "Using shared KVClient instance: " << context_->localClient.get()
             << " (gpu_id: " << args.gpu_id << ")");

        auto ops = std::make_shared<OpPrimitives>(context_->localClient, t_idx_);
        // The verification helpers compare against the thread's data slice.
        ops->SetSharedData(data_);
        // Timed workloads create their CUDA context/stream after the "KPS mode started."
        // line; other workloads set up right away. Init() sizes the pre-allocated pool itself.
        if (HasDeviceOp() && !Timed()) {
            if (ops->Init() != 0) {
                return stats;
            }
        }

        // The single unified loop: every command runs the same way, with count/batch/kps/
        // duration deciding the run length and rate limit. Set → read → Del all run inside
        // each loop round (the kps orchestration), so no separate cleanup phase is needed.
        RunLoop(ops, stats);

        // Push the loop's per-iteration deltas that the last sync did not cover into the
        // shared stats.
        SyncSharedStats(stats, last_synced_);
        return stats;
    }

private:
    // True when the run is time-bounded (kps): runs at `--kps` for `--duration`.
    bool Timed() const { return control_ != nullptr && args.duration_s > 0; }

    bool HasDeviceOp() const {
        return AnyKind(OpKind::MGetH2D) || AnyKind(OpKind::GetH2D);
    }

    bool AnyKind(OpKind kind) const {
        const std::vector<Op>* lists[] = {&workload_.once_ops, &workload_.loop_ops,
                                          &workload_.cleanup_ops};
        for (const std::vector<Op>* list : lists) {
            for (const Op& op : *list) {
                if (op.kind == kind) {
                    return true;
                }
            }
        }
        return false;
    }

    // Stops the shared barrier (when the workload asks for it) and reports a fatal failure.
    int Abort() {
        if (workload_.stop_barrier_on_error && barrier_) {
            barrier_->Stop();
        }
        return -1;
    }

    // Runs one op over `batch`, handling its sync barriers. Returns false on a fatal
    // failure (device/alloc error, missing client, or a stopped barrier) so the caller
    // stops the run. A normal op failure is counted in the stats and returns true; when
    // `skip_rest` is non-null it is set to true if the iteration should stop (a failed
    // Set means the keys were not written, so the following Get/Del are skipped).
    int RunOp(const std::shared_ptr<OpPrimitives>& ops, const Op& op, RunStats& stats,
               const rh2d_batch_data::Data& batch,
               int round, int verify_start, bool* skip_rest = nullptr) {
        if (op.sync_before && barrier_ && barrier_->Wait()) {
            return Abort();
        }
        KVClient* client = context_->Resolve(op.role);
        if (client == nullptr) {
            return Abort();
        }
        switch (op.kind) {
        case OpKind::Set:
            if (ops->Set(*client, batch, stats, round, op.emit_keys) != 0 && skip_rest) {
                *skip_rest = true;
            }
            break;
        case OpKind::Del:
            ops->Del(*client, batch, stats);
            break;
        case OpKind::MGetH2D:
            if (ops->MGetH2D(*client, batch, stats, round, verify_start) != 0) {
                return Abort();
            }
            break;
        case OpKind::GetH2D:
            if (ops->GetH2D(*client, batch, stats, round, verify_start) != 0) {
                return Abort();
            }
            break;
        default:
            return Abort();
        }
        if (op.sync_after && barrier_ && barrier_->Wait()) {
            return Abort();
        }
        return 0;
    }

// Runs the unified loop over the single batch template. `data_` holds one batch of
    // keys/values with a reserved round prefix; every iteration stamps the current round
    // into that prefix and reuses the same batch. Non-timed workloads run count/batch
    // rounds; timed workloads run until `--duration` at `--kps`, wrapping forever.
    void RunLoop(const std::shared_ptr<OpPrimitives>& ops, RunStats& stats) {
        const int total = static_cast<int>(data_.size());
        if (total == 0) {
            return;
        }
        const int batch_size = total;  // the whole template is one batch
        const bool timed = Timed();
        const int rounds = std::max(1, (args.count / args.thread_count) / batch_size);

        if (timed) {
            // Wait for the runner to open the gate, then log the start line and create the
            // CUDA context/stream (keeps the "KPS mode started." ordering).
            while (control_ && !control_->running.load() && !control_->stop.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            if (control_ && control_->stop.load()) {
                return;
            }
            TLOG(t_idx_, "KPS mode started. kps: " << args.kps << ", batch: " << batch_size
                 << (args.use_user_stream ? " (with user stream)" : "")
                 << (AnyKind(OpKind::GetH2D) ? " (using Get+H2D)" : " (using MGetH2D)"));

            if (HasDeviceOp()) {
                if (ops->Init() != 0) {
                    return;
                }
            }
        }

        auto run_start = std::chrono::steady_clock::now();
        // Per-thread round counter: each worker thread's round starts at 0 and increments
        // independently, so every thread stamps its own round sequence into the batch template.
        int round = 0;
        while (true) {
            // End condition: with --duration the run is time-bounded (get/rh2d kps mode) and
            // ignores the count round limit; otherwise it runs `count/thread/batch` rounds.
            if (timed) {
                double elapsed_s = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - run_start).count() / 1000000.0;
                if (elapsed_s >= args.duration_s) {
                    break;
                }
            } else if (round >= rounds) {
                break;
            }

            if (control_ && control_->rate_limiter) {
                control_->rate_limiter->Acquire(batch_size);
            }

            StampRound(round);
            // The verification helpers compare against the current round's data, so sync the
            // stamped template into the op primitives.
            ops->SetSharedData(data_);
            for (const Op& op : workload_.loop_ops) {
                bool skip_rest = false;
                if (RunOp(ops, op, stats, data_, round, /*verify_start=*/0, &skip_rest) != 0) {
                    if (timed && control_) {
                        control_->stop = true;
                    }
                    return;
                }
                if (skip_rest) {
                    break;
                }
            }
            SyncSharedStats(stats, last_synced_);

            round++;
        }

        if (timed) {
            if (control_) {
                control_->stop = true;  // signal the runner's stats reporter
            }
            if (control_ && control_->stats) {
                TLOG(t_idx_, "KPS mode stopped. Total ops: " << control_->stats->TotalOps());
            }
        }
    }

    // Stamps the current round into the reserved prefix of every key and value in the batch
    // template (16 right-aligned digits). Set and Get run with identical arguments, so both
    // sides generate the same round sequence and the stamped keys match; the value prefix
    // carries the per-round stamp that the data verification compares against.
    void StampRound(int round) {
        std::ostringstream oss;
        oss << std::setw(DataProvider::kRoundLen) << std::setfill('0') << round;
        std::string stamp = oss.str();
        for (auto& kv : data_) {
            kv.first.replace(0, DataProvider::kRoundLen, stamp);
            kv.second.replace(0, DataProvider::kRoundLen, stamp);
        }
    }

    // Pushes this thread's per-op deltas since the previous iteration into the shared
    // per-op stats (op counters, failed counters and one latency sample per op per batch).
    void SyncSharedStats(const RunStats& cur, RunStats& last) {
        if (!control_ || !control_->stats) {
            last = cur;
            return;
        }
        auto shared = control_->stats;
        if (cur.set.ops > last.set.ops) {
            int64_t d_ops = cur.set.ops - last.set.ops;
            shared->set.ops += d_ops;
            shared->set.failed += cur.set.failed - last.set.failed;
            shared->set.AddLatency(cur.set.latency.total_time_us - last.set.latency.total_time_us);
        }
        if (cur.get.ops > last.get.ops) {
            int64_t d_ops = cur.get.ops - last.get.ops;
            shared->get.ops += d_ops;
            shared->get.failed += cur.get.failed - last.get.failed;
            shared->get.AddLatency(cur.get.latency.total_time_us - last.get.latency.total_time_us);
        }
        if (cur.del.ops > last.del.ops) {
            int64_t d_ops = cur.del.ops - last.del.ops;
            shared->del.ops += d_ops;
            shared->del.failed += cur.del.failed - last.del.failed;
            shared->del.AddLatency(cur.del.latency.total_time_us - last.del.latency.total_time_us);
        }
        last = cur;
    }

    int t_idx_ = 0;
    std::shared_ptr<Barrier> barrier_;
    std::shared_ptr<ClientContext> context_;
    Workload workload_;
    std::shared_ptr<RunControl> control_;
    rh2d_batch_data::Data data_;
    RunStats last_synced_;  // cumulative stats already pushed to the shared control->stats
};

// ---------------------------------------------------------------------------
// Orchestrator
// ---------------------------------------------------------------------------
// Orchestrates a run: resolves the invocation style and ips, builds the clients/data
// source/plan and runs the unified workload with a uniform reporter (one per-second
// snapshot plus a final one, the same for every command).
class CommandRunner {
public:
    int Run() {
        // ----- clients, data source and workload setup -----
        std::shared_ptr<KVClient> sharedClient = CreateClient(args.localip, "Local KVClient", "[Main]");
        if (sharedClient == nullptr) {
            return -1;
        }

        std::string data_error;
        auto provider = DataProvider::Create(data_error);
        if (provider == nullptr) {
            TERROR(data_error);
            return -1;
        }
        provider->PrintSizeConfig();

        RunPlan plan;
        plan.workload = BuildWorkload();

        // The remote (Set/Del) client is only needed when the workload has Remote-role ops
        // (set/del of the rh2d/get composition). Other commands use the shared client for
        // both roles.
        plan.clients = std::make_shared<ClientContext>();
        plan.clients->localClient = sharedClient;
        plan.clients->remoteClient = sharedClient;
        if (NeedsRemoteClient(plan.workload)) {
            if (args.remoteip != args.localip) {
                plan.clients->remoteClient = CreateClient(args.remoteip, "Remote KVClient", "[Main]");
                if (plan.clients->remoteClient == nullptr) {
                    return -1;
                }
            } else {
                TMAIN("Reusing shared KVClient for Set/Del because remoteip == localip");
            }
        }

        // Every run shares a per-op stats accumulator (fed by all worker threads) and a
        // rate limiter when the run is rate-limited (--kps).
        auto control = std::make_shared<RunControl>();
        control->stats = std::make_shared<RunStats>();
        if (args.kps > 0) {
            control->rate_limiter =
                std::make_shared<RateLimiter>(args.kps, static_cast<int>(args.kps));
        }
        plan.control = control;

        RunWorkload(plan, provider);
        return 0;
    }

private:
    // Builds and initializes a KVClient connected to `host`, reporting progress via
    // `prefix`/`name`.
    std::shared_ptr<KVClient> CreateClient(const std::string& host, const std::string& name,
                                           const std::string& prefix) {
        if (CudaSetDevice(args.gpu_id, prefix) != 0) {
            return nullptr;
        }
        ConnectOptions connectOptions;
        connectOptions.host = host;
        connectOptions.port = args.port;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(args.gpu_id);
        connectOptions.fastTransportMemSize = args.client_options.fast_transport_mem_size;
        connectOptions.enableLocalCache = args.client_options.enable_local_cache;
        connectOptions.enableClientDirectPipelineH2D = args.client_options.enable_client_direct_rh2d;
        connectOptions.clientDirectPipelineH2DThreadNum = args.client_options.client_direct_thread_num;

        auto sharedClient = std::make_shared<KVClient>(connectOptions);
        if (args.client_options.pin) {
            CudaRegisterPinFuncs();
        }
        Status rc = sharedClient->Init();
        if (rc.IsError()) {
            TERROR(prefix << " Failed to init " << name << ": " << rc.GetMsg());
            return nullptr;
        }
        TMAIN(name << " initialized once: " << sharedClient.get() << ", ip=" << host
              << ", port=" << args.port << ", gpu_id=" << args.gpu_id);
        return sharedClient;
    }

    // Maps the parsed command onto a workload: the loop ops are just the command's op
    // combination; batch decides the chunk size. The run length/rate limit come directly
    // from the shared `args` (kps/duration) via the executor.
    Workload BuildWorkload() const {
        Workload w;
        if (args.cmd == "set") {
            w.loop_ops.push_back(Op{OpKind::Set, ClientRole::Local, false, false, /*emit_keys=*/true});
        } else if (args.cmd == "mgeth2d") {
            w.stop_barrier_on_error = true;
            w.loop_ops.push_back(Op{OpKind::MGetH2D, ClientRole::Local});
            if (args.delete_value) {
                w.loop_ops.push_back(Op{OpKind::Del, ClientRole::Local});
            }
        } else if (args.cmd == "originget") {
            w.stop_barrier_on_error = true;
            w.loop_ops.push_back(Op{OpKind::GetH2D, ClientRole::Local});
            if (args.delete_value) {
                w.loop_ops.push_back(Op{OpKind::Del, ClientRole::Local});
            }
        } else if (args.cmd == "rh2d") {
            // Set → MGetH2D → Del all run each round, matching the kps orchestration so each
            // round's Set (round-stamped keys) is written before the local read and deleted
            // afterwards.
            w.loop_ops.push_back(Op{OpKind::Set, ClientRole::Remote});
            w.loop_ops.push_back(Op{OpKind::MGetH2D, ClientRole::Local});
            w.loop_ops.push_back(Op{OpKind::Del, ClientRole::Remote});
        } else if (args.cmd == "get") {
            w.loop_ops.push_back(Op{OpKind::Set, ClientRole::Remote});
            w.loop_ops.push_back(Op{OpKind::GetH2D, ClientRole::Local});
            w.loop_ops.push_back(Op{OpKind::Del, ClientRole::Remote});
        }
        return w;
    }

    // True when the workload operates on the remote client (Remote-role ops).
    bool NeedsRemoteClient(const Workload& w) const {
        const std::vector<Op>* lists[] = {&w.once_ops, &w.loop_ops, &w.cleanup_ops};
        for (const std::vector<Op>* list : lists) {
            for (const Op& op : *list) {
                if (op.role == ClientRole::Remote) {
                    return true;
                }
            }
        }
        return false;
    }

    // Spawns one worker thread per configured thread and runs the unified loop. The run
    // length comes from the shared `args` (one pass over the data, or `--duration` at
    // `--kps`); a uniform reporter prints one stats snapshot per second and one at
    // the end, the same for every command.
    void RunWorkload(const RunPlan& plan, const std::shared_ptr<DataProvider>& provider) {
        auto control = plan.control;
        auto stats = control->stats;

        TMAIN("Starting " << args.thread_count << " threads...");

        auto runing_barrier = std::make_shared<Barrier>(args.thread_count);
        auto start_barrier = std::make_shared<Barrier>(args.thread_count + 1);
        auto main_start = std::chrono::steady_clock::now();
        std::vector<std::thread> threads;
        threads.reserve(args.thread_count);
        for (int t_idx = 0; t_idx < args.thread_count; ++t_idx) {
            threads.emplace_back([&, t_idx, runing_barrier, start_barrier]() {
                auto slice = provider->ThreadSlice(t_idx);
                // Wait until every thread has generated its data ("generate data ok")
                // before any starts running the workload.
                if (start_barrier->Wait()) {
                    return;
                }
                try {
                    Executor executor(t_idx, runing_barrier, plan, slice);
                    executor.Run();
                } catch (const std::exception& e) {
                    TLOG(t_idx, "Error: " << e.what());
                    runing_barrier->Stop();
                }
            });
        }

        // Timed workloads open the running gate once all threads are ready; the executor
        // threads stop themselves after the duration and signal the reporter.
        if (start_barrier->Wait()) {
            return;
        }
        control->running = true;

        // Uniform stats reporter: one snapshot per second while the run is active.
        std::thread stats_thread([control, stats, &main_start]() {
            auto last_print_time = main_start;
            int64_t last_total_ops = 0;
            while (!control->stop.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                if (control->stop.load()) {
                    break;
                }
                auto now = std::chrono::steady_clock::now();
                if (now - last_print_time < std::chrono::seconds(5)) {
                    continue;
                }
                last_print_time = now;
                int64_t total_ops = stats->TotalOps();
                if (total_ops == last_total_ops) {
                    continue;  // no progress since the last snapshot, skip it
                }
                last_total_ops = total_ops;
                double total_s = std::chrono::duration_cast<std::chrono::microseconds>(now - main_start).count() / 1000000.0;
                std::cout << std::endl;
                std::cout << "=================== [" << std::fixed << std::setprecision(1) << total_s << "s] ===================" << std::endl;
                stats->Print(total_s, "[Stats] ");
                std::cout << "=======================================================" << std::endl;
            }
        });

        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
        control->stop = true;
        if (stats_thread.joinable()) stats_thread.join();

        auto main_end = std::chrono::steady_clock::now();
        double total_time_s = std::chrono::duration_cast<std::chrono::microseconds>(main_end - main_start).count() / 1000000.0;

        // Final snapshot, printed once at the end for every command.
        std::cout << std::endl;
        std::cout << "==================== Final Summary ====================" << std::endl;
        std::cout << "Total time: " << std::fixed << std::setprecision(3) << total_time_s << " s" << std::endl;
        std::cout << std::endl;
        stats->Print(total_time_s, "");
        std::cout << "=======================================================" << std::endl;
    }
};

int main(int argc, char* argv[]) {
    args.Parse(argc, argv);

    if (args.help) {
        args.PrintUsage();
        return 0;
    }

    if (args.ValidateAndResolve() != 0) {
        return -1;
    }
    args.Print();

    CommandRunner runner;
    return runner.Run();
}

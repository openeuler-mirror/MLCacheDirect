#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
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
#include <sys/wait.h>
#include <unistd.h>
#include <queue>
#include <deque>
#include <sys/stat.h> 
#include <sys/types.h> 

#ifdef USE_PIPLN_MOCK
#include "mock.h"
#else
#include <cuda_runtime.h>
#endif

using namespace datasystem;

#ifndef USE_PIPLN_MOCK
static cudaError_t CreateH2DStream(cudaStream_t *stream)
{
    return cudaStreamCreateWithFlags(stream, cudaStreamNonBlocking);
}

static cudaError_t WaitAndDestroyH2DStream(cudaStream_t stream)
{
    if (stream == nullptr) {
        return cudaSuccess;
    }
    cudaError_t syncErr = cudaStreamSynchronize(stream);
    cudaError_t destroyErr = cudaStreamDestroy(stream);
    return syncErr != cudaSuccess ? syncErr : destroyErr;
}
#endif

std::mutex print_mutex;

// Token bucket rate limiter
class RateLimiter {
public:
    RateLimiter(double rate, int burst_size = 100)
        : rate_(rate), tokens_(burst_size), max_tokens_(burst_size),
          last_time_(std::chrono::steady_clock::now()) {}

    // Try to acquire n tokens, return wait time in microseconds if need to wait
    int64_t Acquire(int n = 1) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto now = std::chrono::steady_clock::now();
        auto elapsed_us = std::chrono::duration_cast<std::chrono::microseconds>(now - last_time_).count();
        last_time_ = now;

        // Add tokens based on elapsed time
        double new_tokens = (elapsed_us / 1000000.0) * rate_;
        tokens_ = std::min(max_tokens_, tokens_ + new_tokens);

        if (tokens_ >= n) {
            tokens_ -= n;
            return 0;  // No wait needed
        } else {
            // Calculate wait time
            double deficit = n - tokens_;
            int64_t wait_us = static_cast<int64_t>(deficit / rate_ * 1000000.0);
            tokens_ = 0;
            return wait_us;
        }
    }

    void SetRate(double rate) {
        std::lock_guard<std::mutex> lock(mutex_);
        rate_ = rate;
    }

private:
    double rate_;  // tokens per second
    double tokens_;
    double max_tokens_;
    std::chrono::steady_clock::time_point last_time_;
    std::mutex mutex_;
};

// Size configuration for batch
struct SizeConfig {
    size_t size;
    int count;  // -1 means remaining in batch
};

struct ClientOptions {
    uint64_t fast_transport_mem_size = 2 * 1024ULL * 1024 * 1024;
    bool enable_local_cache = true;
    bool enable_client_direct_rh2d = false;
    int client_direct_thread_num = 32;
};

#define TLOG(tid, ...) do { \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cout << "[T" << tid << "] "; \
    std::cout << __VA_ARGS__ << std::endl; \
} while(0)

#define TLOG_NONL(tid, ...) do { \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cout << "[T" << tid << "] "; \
    std::cout << __VA_ARGS__; \
} while(0)

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

bool ParseKeyValue(const std::string& arg, std::string& key, std::string& value) {
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
            cv_.wait(lock, [this, gen] { return gen != generation_; });
        }
        return stop_;
    }
    void Stop()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        stop_ = true;
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int total_;
    int count_;
    int generation_;
    bool stop_;
};

// Enhanced statistics structure with percentile tracking
struct LatencyStats {
    std::vector<double> latencies_us;
    double total_time_us = 0;
    double get_time_us = 0;
    double h2d_time_us = 0;
    int batch_count = 0;
    int key_count = 0;

    void AddLatency(double latency_us) {
        latencies_us.push_back(latency_us);
        total_time_us += latency_us;
    }

    void AddLatencyWithBreakdown(double total_us, double get_us, double h2d_us) {
        latencies_us.push_back(total_us);
        total_time_us += total_us;
        get_time_us += get_us;
        h2d_time_us += h2d_us;
    }

    double GetAvg() const {
        if (latencies_us.empty()) return 0;
        return total_time_us / latencies_us.size();
    }

    double GetPercentile(double percentile) const {
        if (latencies_us.empty()) return 0;
        std::vector<double> sorted = latencies_us;
        std::sort(sorted.begin(), sorted.end());

        double index = (percentile / 100.0) * (sorted.size() - 1);
        size_t lower_idx = static_cast<size_t>(std::floor(index));
        size_t upper_idx = static_cast<size_t>(std::ceil(index));

        if (lower_idx == upper_idx || upper_idx >= sorted.size()) {
            return sorted[lower_idx];
        }

        double fraction = index - lower_idx;
        return sorted[lower_idx] * (1.0 - fraction) + sorted[upper_idx] * fraction;
    }

    double GetP90() const { return GetPercentile(90.0); }
    double GetP95() const { return GetPercentile(95.0); }
    double GetP99() const { return GetPercentile(99.0); }
    double GetP9999() const { return GetPercentile(99.99); }
    double GetMax() const {
        if (latencies_us.empty()) return 0;
        return *std::max_element(latencies_us.begin(), latencies_us.end());
    }
};

struct Stats {
    double total_time_us;
    int batch_count;
    int key_count;
    double get_time_us;
    double h2d_time_us;
    LatencyStats latency_stats;
};

// KPS mode statistics
struct KpsOperationStats {
    std::atomic<int64_t> total_ops{0};
    std::atomic<int64_t> set_ops{0};
    std::atomic<int64_t> get_ops{0};
    std::atomic<int64_t> del_ops{0};
    std::atomic<int64_t> failed_ops{0};

    std::deque<double> set_latencies_us;
    std::deque<double> get_latencies_us;
    std::deque<double> del_latencies_us;
    mutable std::mutex latency_mutex;

    void AddSetLatency(double us) {
        std::lock_guard<std::mutex> lock(latency_mutex);
        set_latencies_us.push_back(us);
        if (set_latencies_us.size() > 100000) {
            set_latencies_us.pop_front();
        }
    }

    void AddGetLatency(double us) {
        std::lock_guard<std::mutex> lock(latency_mutex);
        get_latencies_us.push_back(us);
        if (get_latencies_us.size() > 100000) {
            get_latencies_us.pop_front();
        }
    }

    void AddDelLatency(double us) {
        std::lock_guard<std::mutex> lock(latency_mutex);
        del_latencies_us.push_back(us);
        if (del_latencies_us.size() > 100000) {
            del_latencies_us.pop_front();
        }
    }

    std::vector<double> GetSetLatencies() const {
        std::lock_guard<std::mutex> lock(latency_mutex);
        return std::vector<double>(set_latencies_us.begin(), set_latencies_us.end());
    }

    std::vector<double> GetGetLatencies() const {
        std::lock_guard<std::mutex> lock(latency_mutex);
        return std::vector<double>(get_latencies_us.begin(), get_latencies_us.end());
    }

    std::vector<double> GetDelLatencies() const {
        std::lock_guard<std::mutex> lock(latency_mutex);
        return std::vector<double>(del_latencies_us.begin(), del_latencies_us.end());
    }

    void Reset() {
        total_ops = 0;
        set_ops = 0;
        get_ops = 0;
        del_ops = 0;
        failed_ops = 0;
        std::lock_guard<std::mutex> lock(latency_mutex);
        set_latencies_us.clear();
        get_latencies_us.clear();
        del_latencies_us.clear();
    }
};

// Global atomic flag for stopping KPS test
std::atomic<bool> g_kps_stop(false);
std::atomic<bool> g_kps_running(false);

std::string GenerateRandomString(size_t length) {
    static const char charset[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    static std::mt19937 rng(std::random_device{}());
    static std::uniform_int_distribution<size_t> dist(0, sizeof(charset) - 2);

    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += charset[dist(rng)];
    }
    return result;
}

class RemoteH2DTest {
public:
    RemoteH2DTest(const std::string& host, int port, int thread_id, int gpu_id, bool verify_data = true,
                  const ClientOptions& client_options = ClientOptions())
        : thread_id_(thread_id), gpu_id_(gpu_id), host_(host), port_(port), verify_data_(verify_data),
          client_options_(client_options) {
        client_ = nullptr;
    }

    bool Init() {
        ConnectOptions connectOptions;
        connectOptions.host = host_;
        connectOptions.port = port_;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(gpu_id_);
        connectOptions.fastTransportMemSize = client_options_.fast_transport_mem_size;
        connectOptions.enableLocalCache = client_options_.enable_local_cache;
        connectOptions.enableClientDirectPipelineH2D = client_options_.enable_client_direct_rh2d;
        connectOptions.clientDirectPipelineH2DThreadNum = client_options_.client_direct_thread_num;

        client_ = std::make_unique<KVClient>(connectOptions);
        Status rc = client_->Init();
        if (rc.IsError()) {
            TLOG(thread_id_, "Failed to connect to " << host_ << ":" << port_ << " - " << rc.GetMsg());
            return false;
        }
        TLOG(thread_id_, "Connected to " << host_ << ":" << port_ << " (GPU: " << gpu_id_ << ")");
        return true;
    }

    void SetDataParams(const std::string& valuesize_config) {
        // Parse value size configuration
        // Format: "size1:num1,size2:num2,size3" (size3 without num means remaining)
        value_size_configs_.clear();

        if (valuesize_config.empty()) {
            // Default: single size
            value_size_configs_.push_back({8388608, -1});
            return;
        }

        std::vector<std::string> parts = SplitString(valuesize_config, ',');
        for (const auto& part : parts) {
            std::vector<std::string> size_num = SplitString(part, ':');
            if (size_num.empty()) continue;

            size_t size = std::stoull(size_num[0]);
            int count = -1;  // -1 means remaining

            if (size_num.size() > 1) {
                count = std::stoi(size_num[1]);
            }

            value_size_configs_.push_back({size, count});
        }

        if (value_size_configs_.empty()) {
            value_size_configs_.push_back({8388608, -1});
        }
    }

    void SetDataParams(size_t value_size) {
        value_size_configs_.clear();
        value_size_configs_.push_back({(value_size > 0) ? value_size : 8388608, -1});
    }

    void SetBarrier(std::shared_ptr<Barrier> barrier) { barrier_ = barrier; }

    void SetSharedData(const std::vector<std::pair<std::string, std::string>>& data,
                       int start_idx, int end_idx) {
        data_.clear();
        for (int i = start_idx; i < end_idx; ++i) {
            data_.push_back(data[i]);
        }
    }

    // Generate data with configured sizes for a batch
    void GenerateBatchData(int batch_size) {
        data_.clear();

        // Calculate how many keys for each size
        std::vector<std::pair<size_t, int>> size_counts;
        int allocated = 0;

        for (const auto& config : value_size_configs_) {
            if (config.count == -1) {
                // Remaining keys
                int remaining = batch_size - allocated;
                if (remaining > 0) {
                    size_counts.push_back({config.size, remaining});
                    allocated = batch_size;
                }
            } else {
                int count = std::min(config.count, batch_size - allocated);
                if (count > 0) {
                    size_counts.push_back({config.size, count});
                    allocated += count;
                }
            }
            if (allocated >= batch_size) break;
        }

        // If no valid config, use default
        if (size_counts.empty()) {
            size_counts.push_back({8388608, batch_size});
        }

        // Generate data
        for (size_t idx = 0; idx < size_counts.size(); ++idx) {
            size_t value_size = size_counts[idx].first;
            int count = size_counts[idx].second;

            for (int i = 0; i < count; ++i) {
                std::string key = "key_" + std::to_string(data_.size()) + "_" + GenerateRandomString(8);
                std::string value = GenerateRandomString(value_size);
                data_.emplace_back(key, value);
            }
        }
    }

    void RunRh2DBatch(int batch, Stats& stats, bool use_user_stream = false) {
        if (barrier_ && barrier_->Wait())
            return;

        cudaError_t err;
        if ((err = cudaSetDevice(gpu_id_)) != cudaSuccess) {
            TLOG(thread_id_, "cudaSetDevice(" << gpu_id_ << ") failed: " << cudaGetErrorString(err));
            return;
        }

        int total_keys = static_cast<int>(data_.size());
        int num_batches = total_keys / batch;

        stats.total_time_us = 0;
        stats.batch_count = num_batches;
        stats.key_count = total_keys;
        stats.latency_stats.batch_count = num_batches;
        stats.latency_stats.key_count = total_keys;

#ifdef USE_PIPLN_MOCK
        // Mock mode: always use old interface
        if (use_user_stream) {
            TLOG(thread_id_, "[WARN] use_user_stream is not supported in mock mode, falling back to old interface");
        }
        use_user_stream = false;
#else
        // Create CUDA stream if using new interface
        cudaStream_t h2dStream = nullptr;
        if (use_user_stream) {
            if ((err = CreateH2DStream(&h2dStream)) != cudaSuccess) {
                TLOG(thread_id_, "cudaStreamCreateWithFlags failed: " << cudaGetErrorString(err));
                return;
            }
            TLOG(thread_id_, "Using user-provided CUDA stream for H2D operations");
        }
#endif

        // Pre-allocate CUDA memory if verification is disabled
        std::vector<Blob> preallocated_buffers;
        if (!verify_data_) {
            size_t max_size = 0;
            for (const auto& kv : data_) {
                max_size = std::max(max_size, kv.second.size());
            }
            void* dev_ptr = nullptr;
            if ((err = cudaMalloc(&dev_ptr, max_size)) != cudaSuccess) {
                TLOG(thread_id_, "cudaMalloc failed for pre-allocated buffer: " << cudaGetErrorString(err));
#ifndef USE_PIPLN_MOCK
                if (use_user_stream && h2dStream) {
                    WaitAndDestroyH2DStream(h2dStream);
                }
#endif
                return;
            }
            preallocated_buffers.push_back(Blob{dev_ptr, static_cast<uint64_t>(max_size)});
            TLOG(thread_id_, "Pre-allocated CUDA buffer of size " << max_size << " bytes");
        }

        for (int round = 0; round < num_batches; ++round) {
            std::vector<std::string> keys;
            std::vector<Blob> devShmChunks;
            std::vector<std::string> outFailedKeys;

            int start = round * batch;
            int end = std::min((round + 1) * batch, total_keys);

            for (int i = start; i < end; ++i) {
                keys.push_back(data_[i].first);
                void* dev_ptr = nullptr;

                if (!verify_data_ && !preallocated_buffers.empty()) {
                    // Reuse pre-allocated memory
                    dev_ptr = preallocated_buffers[0].pointer;
                    devShmChunks.push_back(Blob{dev_ptr, static_cast<uint64_t>(data_[i].second.size())});
                } else {
                    // Allocate per-batch (needed for verification)
                    err = cudaMalloc(&dev_ptr, data_[i].second.size());
                    if (err != cudaSuccess) {
                        TLOG(thread_id_, "cudaMalloc failed for key " << data_[i].first
                             << ", size=" << data_[i].second.size()
                             << ", round=" << round << ", i=" << i
                             << ", error: " << cudaGetErrorString(err));
                        for (auto& chunk : devShmChunks) cudaFree(chunk.pointer);
                        if (!preallocated_buffers.empty()) cudaFree(preallocated_buffers[0].pointer);
#ifndef USE_PIPLN_MOCK
                        if (use_user_stream && h2dStream) {
                            WaitAndDestroyH2DStream(h2dStream);
                        }
#endif
                        return;
                    }
                    TLOG(thread_id_, "Allocated device memory for round " << round
                         << ", key " << i << ": ptr=" << dev_ptr
                         << ", size=" << data_[i].second.size());
                    devShmChunks.push_back(Blob{dev_ptr, static_cast<uint64_t>(data_[i].second.size())});
                }
            }

            auto start_time = std::chrono::high_resolution_clock::now();
            Status ret;
#ifndef USE_PIPLN_MOCK
            if (use_user_stream) {
                // Use new interface with user-provided stream and readOnlyBuffer
                std::vector<Optional<ReadOnlyBuffer>> readOnlyBuffers;
                ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys,
                                       reinterpret_cast<void*>(h2dStream), &readOnlyBuffers);
                // Wait for stream to complete
                err = cudaStreamSynchronize(h2dStream);
                if (err != cudaSuccess) {
                    TLOG(thread_id_, "cudaStreamSynchronize failed: " << cudaGetErrorString(err));
                    ret = Status(StatusCode::K_RUNTIME_ERROR, cudaGetErrorString(err));
                }
            } else {
                // Use old interface (default parameters)
                ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys);
            }
#else
            // Mock mode: use old interface
            ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys);
#endif
            auto end_time = std::chrono::high_resolution_clock::now();

            double duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
            stats.latency_stats.AddLatency(duration_us);

            if (ret == datasystem::Status::OK() && outFailedKeys.empty()) {
                TLOG(thread_id_, "Round " << round << " MGetH2D success! Time: " << duration_us << " us"
                     << (use_user_stream ? " (with user stream)" : ""));
            } else {
                TLOG(thread_id_, "Round " << round << " MGetH2D failed. Status: " << ret.GetMsg()
                     << ", Failed keys count: " << outFailedKeys.size());
                if (!outFailedKeys.empty()) {
                    TLOG(thread_id_, "Failed keys:");
                    for (const auto& key : outFailedKeys) {
                        TLOG_NONL(thread_id_, "  " << key << std::endl);
                    }
                }
            }

            if (verify_data_ && outFailedKeys.empty()) {
                CheckDataBatch(start, devShmChunks);
            }

            // Only free if we allocated per-batch
            if (verify_data_) {
                for (auto& chunk : devShmChunks) {
                    cudaFree(chunk.pointer);
                }
            }
        }

#ifndef USE_PIPLN_MOCK
        // Destroy CUDA stream if using new interface
        if (use_user_stream && h2dStream) {
            WaitAndDestroyH2DStream(h2dStream);
        }
#endif

        // Free pre-allocated buffers
        for (auto& buf : preallocated_buffers) {
            cudaFree(buf.pointer);
        }

        stats.total_time_us = stats.latency_stats.total_time_us;
    }

    void RunGetBatch(int batch, Stats& stats) {
        if (barrier_ && barrier_->Wait())
            return;

        cudaError_t err;
        if ((err = cudaSetDevice(gpu_id_)) != cudaSuccess) {
            TLOG(thread_id_, "cudaSetDevice(" << gpu_id_ << ") failed: " << cudaGetErrorString(err));
            return;
        }

        int total_keys = static_cast<int>(data_.size());
        int num_batches = total_keys / batch;

        stats.total_time_us = 0;
        stats.get_time_us = 0;
        stats.h2d_time_us = 0;
        stats.batch_count = num_batches;
        stats.key_count = total_keys;
        stats.latency_stats.batch_count = num_batches;
        stats.latency_stats.key_count = total_keys;

        // Pre-allocate CUDA memory if verification is disabled
        std::vector<void*> preallocated_ptrs;
        if (!verify_data_) {
            size_t max_size = 0;
            for (const auto& kv : data_) {
                max_size = std::max(max_size, kv.second.size() + 1);
            }
            void* dev_ptr = nullptr;
            if ((err = cudaMalloc(&dev_ptr, max_size)) != cudaSuccess) {
                TLOG(thread_id_, "cudaMalloc failed for pre-allocated buffer: " << cudaGetErrorString(err));
                return;
            }
            preallocated_ptrs.push_back(dev_ptr);
            TLOG(thread_id_, "Pre-allocated CUDA buffer of size " << max_size << " bytes for Get+H2D");
        }

        for (int round = 0; round < num_batches; ++round) {
            std::vector<std::string> keys;
            std::vector<void*> dev_ptrs;
            std::vector<std::string> values;

            int start = round * batch;
            int end = std::min((round + 1) * batch, total_keys);

            for (int i = start; i < end; ++i) {
                keys.push_back(data_[i].first);
                void* dev_ptr = nullptr;

                if (!verify_data_ && !preallocated_ptrs.empty()) {
                    // Reuse pre-allocated memory
                    dev_ptr = preallocated_ptrs[0];
                } else {
                    // Allocate per-batch (needed for verification)
                    if ((err = cudaMalloc(&dev_ptr, data_[i].second.size() + 1)) != cudaSuccess) {
                        TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                        for (auto ptr : dev_ptrs) cudaFree(ptr);
                        for (auto ptr : preallocated_ptrs) cudaFree(ptr);
                        return;
                    }
                }
                dev_ptrs.push_back(dev_ptr);
            }

            auto get_start = std::chrono::high_resolution_clock::now();
            Status rc = client_->Get(keys, values, 6000000);
            auto get_end = std::chrono::high_resolution_clock::now();

            double get_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(get_end - get_start).count();

            if (rc.IsError()) {
                TLOG(thread_id_, "Round " << round << " Get failed: " << rc.GetMsg());
                if (verify_data_) {
                    for (auto ptr : dev_ptrs) cudaFree(ptr);
                }
                continue;
            }

            auto h2d_start = std::chrono::high_resolution_clock::now();
            for (size_t i = 0; i < keys.size(); ++i) {
                if ((err = cudaMemcpy(dev_ptrs[i], values[i].c_str(),
                        values[i].length() + 1, cudaMemcpyHostToDevice)) != cudaSuccess) {
                    TLOG(thread_id_, "cudaMemcpy failed for key " << i << ": " << cudaGetErrorString(err));
                }
            }
            auto h2d_end = std::chrono::high_resolution_clock::now();

            double h2d_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(h2d_end - h2d_start).count();
            double total_duration_us = get_duration_us + h2d_duration_us;

            stats.latency_stats.AddLatencyWithBreakdown(total_duration_us, get_duration_us, h2d_duration_us);

            TLOG(thread_id_, "Round " << round << " Get: " << get_duration_us << " us, H2D: " << h2d_duration_us << " us");

            if (verify_data_) {
                CheckDataBatchHost(start, values);
            }

            // Only free if we allocated per-batch
            if (verify_data_) {
                for (auto ptr : dev_ptrs) {
                    cudaFree(ptr);
                }
            }
        }

        // Free pre-allocated buffers
        for (auto ptr : preallocated_ptrs) {
            cudaFree(ptr);
        }

        stats.total_time_us = stats.latency_stats.total_time_us;
        stats.get_time_us = stats.latency_stats.get_time_us;
        stats.h2d_time_us = stats.latency_stats.h2d_time_us;
    }

    // KPS mode: continuously execute set-get-delete at specified rate
    void RunKpsMode(int batch_size, double target_kps, KpsOperationStats& kps_stats,
                    std::shared_ptr<RateLimiter> rate_limiter, std::shared_ptr<Barrier> barrier,
                    bool use_user_stream = false) {
        if (barrier && barrier->Wait())
            return;

#ifndef USE_PIPLN_MOCK
        cudaError_t err;
        if ((err = cudaSetDevice(gpu_id_)) != cudaSuccess) {
            TLOG(thread_id_, "cudaSetDevice(" << gpu_id_ << ") failed: " << cudaGetErrorString(err));
            return;
        }
#else
        // Mock mode: warn if use_user_stream is requested
        if (use_user_stream) {
            TLOG(thread_id_, "[WARN] use_user_stream is not supported in mock mode");
        }
        use_user_stream = false;
#endif

        std::random_device rd;
        std::mt19937 rng(rd());
        int total_keys = static_cast<int>(data_.size());

        if (total_keys < batch_size) {
            TLOG(thread_id_, "Not enough keys for batch size: " << total_keys << " < " << batch_size);
            return;
        }

        // Wait for KPS mode to start
        while (!g_kps_running.load() && !g_kps_stop.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }

        TLOG(thread_id_, "KPS mode started. Target: " << target_kps << " kps, batch: " << batch_size
             << (use_user_stream ? " (with user stream)" : ""));

        std::uniform_int_distribution<int> key_dist(0, total_keys - batch_size);

#ifndef USE_PIPLN_MOCK
        // Create CUDA stream if using user stream
        cudaStream_t h2dStream = nullptr;
        if (use_user_stream) {
            if ((err = CreateH2DStream(&h2dStream)) != cudaSuccess) {
                TLOG(thread_id_, "cudaStreamCreateWithFlags failed: " << cudaGetErrorString(err));
                return;
            }
            TLOG(thread_id_, "Created user-provided CUDA stream for H2D operations");
        }

        // Pre-allocate CUDA memory for multiple batches
        // Allocate 4 batches worth of memory buffers
        constexpr int NUM_PREALLOCATED_BATCHES = 4;
        std::vector<std::vector<void*>> preallocated_batches(NUM_PREALLOCATED_BATCHES);
        std::vector<size_t> batch_sizes(NUM_PREALLOCATED_BATCHES);

        // Find max value size across all data
        size_t max_value_size = 0;
        for (const auto& kv : data_) {
            max_value_size = std::max(max_value_size, kv.second.size());
        }

        // Pre-allocate memory for each batch
        for (int b = 0; b < NUM_PREALLOCATED_BATCHES; ++b) {
            preallocated_batches[b].resize(batch_size);
            batch_sizes[b] = max_value_size;
            bool alloc_success = true;
            for (int i = 0; i < batch_size; ++i) {
                if ((err = cudaMalloc(&preallocated_batches[b][i], max_value_size)) != cudaSuccess) {
                    TLOG(thread_id_, "cudaMalloc failed for batch " << b << " slot " << i
                         << ": " << cudaGetErrorString(err));
                    alloc_success = false;
                    break;
                }
            }
            if (!alloc_success) {
                // Free any allocated memory for this batch
                for (int i = 0; i < batch_size; ++i) {
                    if (preallocated_batches[b][i]) {
                        cudaFree(preallocated_batches[b][i]);
                        preallocated_batches[b][i] = nullptr;
                    }
                }
                preallocated_batches[b].clear();
            }
        }

        TLOG(thread_id_, "Pre-allocated " << NUM_PREALLOCATED_BATCHES << " batches of CUDA memory, "
             << batch_size << " buffers per batch, " << max_value_size << " bytes each");

        std::uniform_int_distribution<int> batch_dist(0, NUM_PREALLOCATED_BATCHES - 1);
#endif

        while (!g_kps_stop.load()) {
            // Apply rate limiting (3 ops per batch: set + rh2d + del)
            int64_t wait_us = rate_limiter->Acquire(batch_size * 3);
            if (wait_us > 0) {
                std::this_thread::sleep_for(std::chrono::microseconds(wait_us));
            }

            // Randomly select starting index for batch
            int start_idx = key_dist(rng);

            // ========== Phase 1: Batch Set ==========
            std::vector<std::string> keys;
            std::vector<std::string> values;
            for (int i = 0; i < batch_size; ++i) {
                keys.push_back(data_[start_idx + i].first);
                values.push_back(data_[start_idx + i].second);
            }

            auto set_start = std::chrono::high_resolution_clock::now();
            std::vector<std::string> setFailedKeys;
            Status rc;
            for (size_t i = 0; i < keys.size(); ++i) {
                Status s = client_->Set(keys[i], values[i]);
                if (s.IsError()) {
                    setFailedKeys.push_back(keys[i]);
                    rc = s;  // Keep last error
                }
            }
            auto set_end = std::chrono::high_resolution_clock::now();
            double set_us = std::chrono::duration_cast<std::chrono::microseconds>(set_end - set_start).count();
            kps_stats.AddSetLatency(set_us);
            kps_stats.set_ops += batch_size;
            kps_stats.total_ops += batch_size;

            if (rc.IsError() || !setFailedKeys.empty()) {
                kps_stats.failed_ops += batch_size;
                TLOG(thread_id_, "Set failed: " << rc.GetMsg() << ", failed keys count: " << setFailedKeys.size());
                if (!setFailedKeys.empty()) {
                    TLOG(thread_id_, "Set failed keys: ");
                    for (const auto& key : setFailedKeys) {
                        TLOG_NONL(thread_id_, "  " << key << std::endl);
                    }
                }
                if (rc.IsError()) continue;
            }

            // ========== Phase 2: Batch MGetH2D ==========
            std::vector<Blob> devShmChunks;
            std::vector<std::string> outFailedKeys;

#ifndef USE_PIPLN_MOCK
            // Randomly select a pre-allocated batch buffer
            int batch_slot = -1;
            for (int b = 0; b < NUM_PREALLOCATED_BATCHES; ++b) {
                if (!preallocated_batches[b].empty()) {
                    batch_slot = b;
                    break;
                }
            }

            if (batch_slot == -1) {
                // No pre-allocated batch available, allocate on-the-fly
                for (int i = 0; i < batch_size; ++i) {
                    void* dev_ptr = nullptr;
                    if ((err = cudaMalloc(&dev_ptr, data_[start_idx + i].second.size())) != cudaSuccess) {
                        TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                        break;
                    }
                    devShmChunks.push_back(Blob{dev_ptr, static_cast<uint64_t>(data_[start_idx + i].second.size())});
                }
            } else {
                // Use pre-allocated batch
                for (int i = 0; i < batch_size; ++i) {
                    devShmChunks.push_back(Blob{
                        preallocated_batches[batch_slot][i],
                        static_cast<uint64_t>(data_[start_idx + i].second.size())
                    });
                }
            }
#else
            // Mock mode: no CUDA operations
            for (int i = 0; i < batch_size; ++i) {
                devShmChunks.push_back(Blob{nullptr, static_cast<uint64_t>(data_[start_idx + i].second.size())});
            }
#endif

            auto rh2d_start = std::chrono::high_resolution_clock::now();
#ifndef USE_PIPLN_MOCK
            if (use_user_stream) {
                // Use new interface with user-provided stream and readOnlyBuffer
                std::vector<Optional<ReadOnlyBuffer>> readOnlyBuffers;
                rc = client_->MGetH2D(keys, devShmChunks, outFailedKeys,
                                      reinterpret_cast<void*>(h2dStream), &readOnlyBuffers);
                // Wait for stream to complete
                err = cudaStreamSynchronize(h2dStream);
                if (err != cudaSuccess) {
                    TLOG(thread_id_, "cudaStreamSynchronize failed: " << cudaGetErrorString(err));
                    rc = Status(StatusCode::K_RUNTIME_ERROR, cudaGetErrorString(err));
                }
            } else {
                // Use old interface (default parameters)
                rc = client_->MGetH2D(keys, devShmChunks, outFailedKeys);
            }
#else
            // Mock mode: use old interface
            rc = client_->MGetH2D(keys, devShmChunks, outFailedKeys);
#endif
            auto rh2d_end = std::chrono::high_resolution_clock::now();
            double rh2d_us = std::chrono::duration_cast<std::chrono::microseconds>(rh2d_end - rh2d_start).count();
            kps_stats.AddGetLatency(rh2d_us);  // Reuse get latency for rh2d
            kps_stats.get_ops += batch_size;
            kps_stats.total_ops += batch_size;

            if (rc.IsError() || !outFailedKeys.empty()) {
                kps_stats.failed_ops += batch_size;
                TLOG(thread_id_, "MGetH2D failed: " << rc.GetMsg() << ", failed keys count: " << outFailedKeys.size());
                if (!outFailedKeys.empty()) {
                    TLOG(thread_id_, "MGetH2D failed keys: ");
                    for (const auto& key : outFailedKeys) {
                        TLOG_NONL(thread_id_, "  " << key << std::endl);
                    }
                }
            }

#ifndef USE_PIPLN_MOCK
            // Free per-batch CUDA memory only if allocated on-the-fly (batch_slot == -1)
            if (batch_slot == -1) {
                for (auto& chunk : devShmChunks) {
                    if (chunk.pointer) cudaFree(chunk.pointer);
                }
            }
#endif

            // ========== Phase 3: Batch Delete ==========
            auto del_start = std::chrono::high_resolution_clock::now();
            std::vector<std::string> delFailedKeys;
            for (const auto& key : keys) {
                Status s = client_->Del(key);
                if (s.IsError()) {
                    delFailedKeys.push_back(key);
                    rc = s;  // Keep last error
                }
            }
            auto del_end = std::chrono::high_resolution_clock::now();
            double del_us = std::chrono::duration_cast<std::chrono::microseconds>(del_end - del_start).count();
            kps_stats.AddDelLatency(del_us);
            kps_stats.del_ops += batch_size;
            kps_stats.total_ops += batch_size;

            if (rc.IsError() || !delFailedKeys.empty()) {
                kps_stats.failed_ops += batch_size;
                TLOG(thread_id_, "Del failed: " << rc.GetMsg() << ", failed keys count: " << delFailedKeys.size());
                if (!delFailedKeys.empty()) {
                    TLOG(thread_id_, "Del failed keys: ");
                    for (const auto& key : delFailedKeys) {
                        TLOG_NONL(thread_id_, "  " << key << std::endl);
                    }
                }
            }
        }

#ifndef USE_PIPLN_MOCK
        // Free all pre-allocated CUDA memory
        for (int b = 0; b < NUM_PREALLOCATED_BATCHES; ++b) {
            for (auto ptr : preallocated_batches[b]) {
                if (ptr) cudaFree(ptr);
            }
        }

        // Destroy CUDA stream if using user stream
        if (use_user_stream && h2dStream) {
            WaitAndDestroyH2DStream(h2dStream);
        }
#endif

        TLOG(thread_id_, "KPS mode stopped. Total ops: " << kps_stats.total_ops.load());
    }

    // Get the underlying client pointer (for multi-process KPS mode)
    KVClient* GetClient() { return client_.get(); }

private:
    std::vector<SizeConfig> value_size_configs_;

    void CheckDataBatch(int startIdx, const std::vector<Blob>& devShmChunks) {
#ifdef USE_PIPLN_MOCK
        TLOG(thread_id_, "CheckDataBatch skipped in mock mode (no real GPU memory to verify)");
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
                TLOG(thread_id_, "cudaMemcpy D2H failed for " << (startIdx + i) << "th: " << cudaGetErrorString(err));
                is_failed = true;
                continue;
            }
            if (readOutData != expected) {
                TLOG(thread_id_, "Data mismatch at index " << (startIdx + i));
                is_failed = true;
            }
        }
        if (is_failed) {
            TLOG(thread_id_, "CheckDataBatch FAILED!");
        } else {
            TLOG(thread_id_, "CheckDataBatch passed!");
        }
#endif
    }

    void CheckDataBatchHost(int startIdx, const std::vector<std::string>& values) {
#ifdef USE_PIPLN_MOCK
        TLOG(thread_id_, "CheckDataBatch skipped in mock mode (no real GPU memory to verify)");
#else
        bool is_failed = false;
        for (size_t i = 0; i < values.size(); i++) {
            auto& expected = data_[startIdx + i].second;
            if (values[i] != expected) {
                TLOG(thread_id_, "Data mismatch at index " << (startIdx + i));
                is_failed = true;
            }
        }
        if (is_failed) {
            TLOG(thread_id_, "CheckDataBatchHost FAILED!");
        } else {
            TLOG(thread_id_, "CheckDataBatchHost passed!");
        }
#endif
    }

    std::unique_ptr<KVClient> client_;
    std::vector<std::pair<std::string, std::string>> data_;
    size_t value_size_ = 8388608;
    int thread_id_ = 0;
    int gpu_id_ = 0;
    std::string host_;
    int port_;
    std::shared_ptr<Barrier> barrier_;
    bool verify_data_ = true;
    ClientOptions client_options_;
};

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <command> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  rh2d : Set data to remote, then MGetH2D from local" << std::endl;
    std::cout << "  get  : Set data to remote, then Get and copy to CUDA" << std::endl;
    std::cout << "  kps  : KPS mode: continuously set-get-delete at specified rate" << std::endl;
    std::cout << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --count=N         Total keys (default: 100)" << std::endl;
    std::cout << "  --batch=N         Keys per batch (default: 10)" << std::endl;
    std::cout << "  --thread=N        Threads (default: 1)" << std::endl;
    std::cout << "  --process=N       Processes (default: 0, 0=multi-thread)" << std::endl;
    std::cout << "  --kps=N           Target KPS (keys per second) for kps mode (default: 0)" << std::endl;
    std::cout << "  --duration=N      Duration in seconds for kps mode (default: 60)" << std::endl;
    std::cout << "  --valuesize=CFG   Value size config (default: 8388608)" << std::endl;
    std::cout << "                    Format: size1:num1,size2:num2,size3" << std::endl;
    std::cout << "                    Last size without :num = remaining" << std::endl;
    std::cout << "  --remoteip=IP     Remote server IP (required)" << std::endl;
    std::cout << "  --localip=IP      Local IP (default: same as remoteip)" << std::endl;
    std::cout << "  --port=N          Server port (default: 18481)" << std::endl;
    std::cout << "  --gpu_id=N        GPU device ID (default: 0)" << std::endl;
    std::cout << "  --verify=Y/N      Verify data (default: Y)" << std::endl;
    std::cout << "  --use_user_stream=Y/N  Use new MGetH2D interface (default: N)" << std::endl;
    std::cout << "  --enable_local_cache=Y/N  Enable datasystem local cache (default: Y)" << std::endl;
    std::cout << "  --enable_client_direct_rh2d=Y/N  Enable client-direct RH2D (default: N)" << std::endl;
    std::cout << "  --client_direct_thread_num=N  Client-direct RH2D thread count (default: 32)" << std::endl;
    std::cout << "  --fast_transport_mem_size=N   Fast transport memory size (default: 2147483648)" << std::endl;
    std::cout << "  --help, -h        Show this help" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  # Basic test (4 threads, 1MB values)" << std::endl;
    std::cout << "  " << prog << " rh2d --count=100 --batch=10 --thread=4 --valuesize=1048576 --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
    std::cout << std::endl;
    std::cout << "  # Mixed sizes (batch=10: 3x1KB + 2x4KB + 5x8KB)" << std::endl;
    std::cout << "  " << prog << " rh2d --count=100 --batch=10 --valuesize=1024:3,4096:2,8192 --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
    std::cout << std::endl;
    std::cout << "  # New interface with user stream" << std::endl;
    std::cout << "  " << prog << " rh2d --count=100 --batch=10 --use_user_stream=Y --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
    std::cout << std::endl;
    std::cout << "  # Client-direct RH2D without local worker" << std::endl;
    std::cout << "  " << prog << " rh2d --count=100 --batch=10 --thread=4 --value_size=3670016 \\" << std::endl;
    std::cout << "    --remoteip=192.168.1.100 --enable_local_cache=N \\" << std::endl;
    std::cout << "    --enable_client_direct_rh2d=Y --client_direct_thread_num=64" << std::endl;
    std::cout << std::endl;
    std::cout << "  # KPS mode: 1000 ops/sec, 4 threads, 60 seconds" << std::endl;
    std::cout << "  " << prog << " kps --count=100 --batch=10 --kps=1000 --thread=4 --duration=60 --remoteip=192.168.1.100" << std::endl;
}

struct CmdArgs {
    std::string cmd;
    int count = 100;
    int batch = 10;
    int thread_count = 1;
    int process_count = 0;
    double kps = 0;  // Target keys per second for kps mode
    int duration = 60;  // Duration in seconds for kps mode
    std::string valuesize_config;  // Format: "size1:num1,size2:num2,size3"
    size_t value_size = 8388608;   // Legacy single size (used if valuesize_config empty)
    std::string remoteip;
    std::string localip;
    int port = 18481;
    int gpu_id = 0;
    bool verify_data = true;
    bool use_user_stream = false;
    ClientOptions client_options;
    bool help = false;
};

bool ParseBoolValue(std::string value)
{
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    return value == "y" || value == "yes" || value == "1" || value == "true";
}

void ApplyOption(CmdArgs& args, const std::string& key, const std::string& value)
{
    if (key == "count") args.count = std::stoi(value);
    else if (key == "batch") args.batch = std::stoi(value);
    else if (key == "thread") args.thread_count = std::stoi(value);
    else if (key == "process") args.process_count = std::stoi(value);
    else if (key == "kps") args.kps = std::stod(value);
    else if (key == "duration") args.duration = std::stoi(value);
    else if (key == "valuesize" || key == "value_size") args.valuesize_config = value;
    else if (key == "remoteip") args.remoteip = value;
    else if (key == "localip") args.localip = value;
    else if (key == "port") args.port = std::stoi(value);
    else if (key == "gpu_id") args.gpu_id = std::stoi(value);
    else if (key == "verify") args.verify_data = ParseBoolValue(value);
    else if (key == "use_user_stream") args.use_user_stream = ParseBoolValue(value);
    else if (key == "enable_local_cache") args.client_options.enable_local_cache = ParseBoolValue(value);
    else if (key == "enable_client_direct_rh2d") {
        args.client_options.enable_client_direct_rh2d = ParseBoolValue(value);
    } else if (key == "client_direct_thread_num") {
        args.client_options.client_direct_thread_num = std::stoi(value);
    } else if (key == "fast_transport_mem_size") {
        args.client_options.fast_transport_mem_size = std::stoull(value);
    }
}

CmdArgs ParseArgs(int argc, char* argv[]) {
    CmdArgs args;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];
        std::string key, value;

        if (arg == "--help" || arg == "-h") {
            args.help = true;
            return args;
        }

        if (ParseKeyValue(arg, key, value)) {
            ApplyOption(args, key, value);
            continue;
        }

        if (arg.size() >= 3 && arg[0] == '-' && arg[1] == '-') {
            key = arg.substr(2);
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                value = argv[++i];
                ApplyOption(args, key, value);
            }
        } else if (arg[0] != '-') {
            if (args.cmd.empty()) {
                args.cmd = arg;
            }
        }
    }

    return args;
}

void PrintLatencyStats(const LatencyStats& stats, const std::string& prefix = "") {
    std::cout << std::fixed << std::setprecision(2);
    std::cout << prefix << "Latency Statistics:" << std::endl;
    std::cout << prefix << "  Avg:     " << std::setw(10) << (stats.GetAvg() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  P90:     " << std::setw(10) << (stats.GetP90() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  P95:     " << std::setw(10) << (stats.GetP95() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  P99:     " << std::setw(10) << (stats.GetP99() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  P99.99:  " << std::setw(10) << (stats.GetP9999() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  MAX:     " << std::setw(10) << (stats.GetMax() / 1000.0) << " ms" << std::endl;
}

// Calculate percentile from a vector of latencies
double CalculatePercentile(const std::vector<double>& latencies, double percentile) {
    if (latencies.empty()) return 0;
    std::vector<double> sorted = latencies;
    std::sort(sorted.begin(), sorted.end());

    double index = (percentile / 100.0) * (sorted.size() - 1);
    size_t lower_idx = static_cast<size_t>(std::floor(index));
    size_t upper_idx = static_cast<size_t>(std::ceil(index));

    if (lower_idx == upper_idx || upper_idx >= sorted.size()) {
        return sorted[lower_idx];
    }

    double fraction = index - lower_idx;
    return sorted[lower_idx] * (1.0 - fraction) + sorted[upper_idx] * fraction;
}

// Print KPS statistics
void PrintKpsStats(const KpsOperationStats& stats, double interval_s, const std::string& prefix = "") {
    double actual_kps = interval_s > 0 ? stats.total_ops.load() / interval_s : 0;
    double set_kps = interval_s > 0 ? stats.set_ops.load() / interval_s : 0;
    double get_kps = interval_s > 0 ? stats.get_ops.load() / interval_s : 0;
    double del_kps = interval_s > 0 ? stats.del_ops.load() / interval_s : 0;

    std::cout << std::fixed << std::setprecision(1);
    std::cout << prefix << "Actual KPS: " << actual_kps
              << " (set: " << set_kps
              << ", get: " << get_kps
              << ", del: " << del_kps << ")" << std::endl;

    std::cout << prefix << "Total ops: " << stats.total_ops.load()
              << " (set: " << stats.set_ops.load()
              << ", get: " << stats.get_ops.load()
              << ", del: " << stats.del_ops.load()
              << ", failed: " << stats.failed_ops.load() << ")" << std::endl;

    // Get latencies for percentile calculation
    std::vector<double> set_lats = stats.GetSetLatencies();
    std::vector<double> get_lats = stats.GetGetLatencies();
    std::vector<double> del_lats = stats.GetDelLatencies();

    std::cout << std::fixed << std::setprecision(3);
    std::cout << prefix << "Set latency (us): ";
    if (!set_lats.empty()) {
        std::cout << "avg=" << (CalculatePercentile(set_lats, 50.0))
                  << ", P90=" << CalculatePercentile(set_lats, 90.0)
                  << ", P95=" << CalculatePercentile(set_lats, 95.0)
                  << ", P99=" << CalculatePercentile(set_lats, 99.0);
    } else {
        std::cout << "N/A";
    }
    std::cout << std::endl;

    std::cout << prefix << "Get latency (us): ";
    if (!get_lats.empty()) {
        std::cout << "avg=" << (CalculatePercentile(get_lats, 50.0))
                  << ", P90=" << CalculatePercentile(get_lats, 90.0)
                  << ", P95=" << CalculatePercentile(get_lats, 95.0)
                  << ", P99=" << CalculatePercentile(get_lats, 99.0);
    } else {
        std::cout << "N/A";
    }
    std::cout << std::endl;

    std::cout << prefix << "Del latency (us): ";
    if (!del_lats.empty()) {
        std::cout << "avg=" << (CalculatePercentile(del_lats, 50.0))
                  << ", P90=" << CalculatePercentile(del_lats, 90.0)
                  << ", P95=" << CalculatePercentile(del_lats, 95.0)
                  << ", P99=" << CalculatePercentile(del_lats, 99.0);
    } else {
        std::cout << "N/A";
    }
    std::cout << std::endl;
}

// Shared memory structure for multi-process statistics
struct SharedStats {
    double total_time_us;
    double get_time_us;
    double h2d_time_us;
    int batch_count;
    int key_count;
    int process_id;
    // For percentile calculation, we'll use a file-based approach
    static constexpr size_t MAX_LATENCIES = 100000;
    size_t latency_count;
    double latencies[MAX_LATENCIES];
};

void RunMultiProcess(const CmdArgs& args, const std::vector<std::pair<std::string, std::string>>& all_data) {
    int process_count = args.process_count;
    int keys_per_process = args.count / process_count;

    std::cout << "[Main] Starting " << process_count << " processes..." << std::endl;
    std::cout << "[Main] use_user_stream: " << (args.use_user_stream ? "Yes" : "No") << std::endl;

    auto main_start = std::chrono::high_resolution_clock::now();

    std::vector<pid_t> pids(process_count);
    std::vector<std::string> stat_files(process_count);

    for (int pid_idx = 0; pid_idx < process_count; ++pid_idx) {
        pid_t pid = fork();

        if (pid < 0) {
            std::cerr << "Fork failed for process " << pid_idx << std::endl;
            continue;
        }

        if (pid == 0) {
            // Child process
            RemoteH2DTest test(args.localip, args.port, pid_idx, args.gpu_id, args.verify_data,
                               args.client_options);
            if (!test.Init()) {
                exit(1);
            }

            int start_idx = pid_idx * keys_per_process;
            int end_idx = start_idx + keys_per_process;
            test.SetSharedData(all_data, start_idx, end_idx);

            Stats stats;
            if (args.cmd == "rh2d") {
                test.RunRh2DBatch(args.batch, stats, args.use_user_stream);
            } else if (args.cmd == "get") {
                test.RunGetBatch(args.batch, stats);
            }

            // Write statistics to temp file
            std::string stat_file = "/tmp/rh2d_stats_" + std::to_string(pid_idx) + ".tmp";
            std::ofstream ofs(stat_file, std::ios::binary);
            if (ofs) {
                ofs.write(reinterpret_cast<const char*>(&stats.total_time_us), sizeof(double));
                ofs.write(reinterpret_cast<const char*>(&stats.get_time_us), sizeof(double));
                ofs.write(reinterpret_cast<const char*>(&stats.h2d_time_us), sizeof(double));
                ofs.write(reinterpret_cast<const char*>(&stats.batch_count), sizeof(int));
                ofs.write(reinterpret_cast<const char*>(&stats.key_count), sizeof(int));
                size_t latency_count = stats.latency_stats.latencies_us.size();
                ofs.write(reinterpret_cast<const char*>(&latency_count), sizeof(size_t));
                for (const auto& lat : stats.latency_stats.latencies_us) {
                    ofs.write(reinterpret_cast<const char*>(&lat), sizeof(double));
                }
                ofs.close();
            }

            exit(0);
        } else {
            // Parent process
            pids[pid_idx] = pid;
            stat_files[pid_idx] = "/tmp/rh2d_stats_" + std::to_string(pid_idx) + ".tmp";
        }
    }

    // Wait for all processes
    for (int i = 0; i < process_count; ++i) {
        int status;
        waitpid(pids[i], &status, 0);
    }

    auto main_end = std::chrono::high_resolution_clock::now();
    double total_time_s = std::chrono::duration_cast<std::chrono::microseconds>(main_end - main_start).count() / 1000000.0;

    // Collect statistics from all processes
    std::vector<Stats> process_stats(process_count);
	    for (int i = 0; i < process_count; ++i) {
        std::ifstream ifs(stat_files[i], std::ios::binary);
        if (ifs) {
            ifs.read(reinterpret_cast<char*>(&process_stats[i].total_time_us), sizeof(double));
            ifs.read(reinterpret_cast<char*>(&process_stats[i].get_time_us), sizeof(double));
            ifs.read(reinterpret_cast<char*>(&process_stats[i].h2d_time_us), sizeof(double));
            ifs.read(reinterpret_cast<char*>(&process_stats[i].batch_count), sizeof(int));
            ifs.read(reinterpret_cast<char*>(&process_stats[i].key_count), sizeof(int));
            size_t latency_count;
            ifs.read(reinterpret_cast<char*>(&latency_count), sizeof(size_t));
            process_stats[i].latency_stats.latencies_us.resize(latency_count);
            for (size_t j = 0; j < latency_count; ++j) {
                ifs.read(reinterpret_cast<char*>(&process_stats[i].latency_stats.latencies_us[j]), sizeof(double));
            }
            ifs.close();
            unlink(stat_files[i].c_str());
        }
    }

    // Print summary
    std::cout << std::endl;
    std::cout << "==================== Summary ====================" << std::endl;
    std::cout << "Total keys: " << args.count << std::endl;
    std::cout << "Batch size: " << args.batch << std::endl;
    std::cout << "Processes: " << process_count << std::endl;
    std::cout << "Verify data: " << (args.verify_data ? "Yes" : "No") << std::endl;
    std::cout << "Total time: " << total_time_s << " s" << std::endl;
    std::cout << std::endl;

    // Aggregate all latencies for overall statistics
    LatencyStats overall_stats;
    double sum_get_time_us = 0;
    double sum_h2d_time_us = 0;

    for (int pid = 0; pid < process_count; ++pid) {
        const auto& s = process_stats[pid];
        for (const auto& lat : s.latency_stats.latencies_us) {
            overall_stats.latencies_us.push_back(lat);
        }
        overall_stats.total_time_us += s.total_time_us;
        overall_stats.batch_count += s.batch_count;
        overall_stats.key_count += s.key_count;

        if (args.cmd == "get") {
            sum_get_time_us += s.get_time_us;
            sum_h2d_time_us += s.h2d_time_us;
        }

        std::cout << "[P" << pid << "] Keys: " << s.key_count
                  << ", Batches: " << s.batch_count << std::endl;
        PrintLatencyStats(s.latency_stats, "[P" + std::to_string(pid) + "] ");
        if (args.cmd == "get") {
            double avg_get_us = s.batch_count > 0 ? s.get_time_us / s.batch_count : 0;
            double avg_h2d_us = s.batch_count > 0 ? s.h2d_time_us / s.batch_count : 0;
            std::cout << "[P" << pid << "] Avg Get Time: " << std::fixed << std::setprecision(2)
                      << (avg_get_us/1000.0) << " ms" << std::endl;
            std::cout << "[P" << pid << "] Avg H2D Time: " << std::fixed << std::setprecision(2)
                      << (avg_h2d_us/1000.0) << " ms" << std::endl;
        }
        std::cout << std::endl;
    }

    std::cout << "==================== Overall ====================" << std::endl;
    PrintLatencyStats(overall_stats, "");

    double overall_qps = overall_stats.total_time_us > 0 ? (overall_stats.key_count * 1000000.0 / overall_stats.total_time_us) : 0;
    std::cout << "Overall QPS: " << std::fixed << std::setprecision(1) << overall_qps << " keys/s" << std::endl;

    if (args.cmd == "get") {
        std::cout << "Overall Avg Get Time: " << std::fixed << std::setprecision(2)
                  << (sum_get_time_us / process_count / 1000.0) << " ms" << std::endl;
        std::cout << "Overall Avg H2D Time: " << std::fixed << std::setprecision(2)
                  << (sum_h2d_time_us / process_count / 1000.0) << " ms" << std::endl;
    }
    std::cout << "=================================================" << std::endl;
}

void RunMultiThread(const CmdArgs& args, const std::vector<std::pair<std::string, std::string>>& all_data) {
    int keys_per_thread = args.count / args.thread_count;

    auto barrier = std::make_shared<Barrier>(args.thread_count);
    std::vector<std::thread> threads;
    std::vector<Stats> thread_stats(args.thread_count);

    std::cout << "[Main] Starting " << args.thread_count << " threads..." << std::endl;
    std::cout << "[Main] use_user_stream: " << (args.use_user_stream ? "Yes" : "No") << std::endl;
    auto main_start = std::chrono::high_resolution_clock::now();

    for (int tid = 0; tid < args.thread_count; ++tid) {
        threads.emplace_back([tid, &args, &all_data, keys_per_thread, &thread_stats, barrier]() {
            RemoteH2DTest test(args.localip, args.port, tid, args.gpu_id, args.verify_data, args.client_options);
            if (!test.Init()) {
                return;
            }
            test.SetBarrier(barrier);

            int start_idx = tid * keys_per_thread;
            int end_idx = start_idx + keys_per_thread;
            test.SetSharedData(all_data, start_idx, end_idx);

            Stats stats;
            if (args.cmd == "rh2d") {
                test.RunRh2DBatch(args.batch, stats, args.use_user_stream);
            } else if (args.cmd == "get") {
                test.RunGetBatch(args.batch, stats);
            }
            thread_stats[tid] = stats;
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    auto main_end = std::chrono::high_resolution_clock::now();
    double total_time_s = std::chrono::duration_cast<std::chrono::microseconds>(main_end - main_start).count() / 1000000.0;

    std::cout << std::endl;
    std::cout << "==================== Summary ====================" << std::endl;
    std::cout << "Total keys: " << args.count << std::endl;
    std::cout << "Batch size: " << args.batch << std::endl;
    std::cout << "Threads: " << args.thread_count << std::endl;
    std::cout << "Verify data: " << (args.verify_data ? "Yes" : "No") << std::endl;
    std::cout << "Total time: " << total_time_s << " s" << std::endl;
    std::cout << std::endl;

    // Aggregate all latencies for overall statistics
    LatencyStats overall_stats;
    double sum_get_time_us = 0;
    double sum_h2d_time_us = 0;

    for (int tid = 0; tid < args.thread_count; ++tid) {
        const auto& s = thread_stats[tid];
        for (const auto& lat : s.latency_stats.latencies_us) {
            overall_stats.latencies_us.push_back(lat);
        }
        overall_stats.total_time_us += s.total_time_us;
        overall_stats.batch_count += s.batch_count;
        overall_stats.key_count += s.key_count;

        if (args.cmd == "get") {
            sum_get_time_us += s.get_time_us;
            sum_h2d_time_us += s.h2d_time_us;
        }

        std::cout << "[T" << tid << "] Keys: " << s.key_count
                  << ", Batches: " << s.batch_count << std::endl;
        PrintLatencyStats(s.latency_stats, "[T" + std::to_string(tid) + "] ");
        if (args.cmd == "get") {
            double avg_get_us = s.batch_count > 0 ? s.get_time_us / s.batch_count : 0;
            double avg_h2d_us = s.batch_count > 0 ? s.h2d_time_us / s.batch_count : 0;
            std::cout << "[T" << tid << "] Avg Get Time: " << std::fixed << std::setprecision(2)
                      << (avg_get_us/1000.0) << " ms" << std::endl;
            std::cout << "[T" << tid << "] Avg H2D Time: " << std::fixed << std::setprecision(2)
                      << (avg_h2d_us/1000.0) << " ms" << std::endl;
        }
        std::cout << std::endl;
    }

    std::cout << "==================== Overall ====================" << std::endl;
    PrintLatencyStats(overall_stats, "");

    double overall_qps = overall_stats.total_time_us > 0 ? (overall_stats.key_count * 1000000.0 / overall_stats.total_time_us) : 0;
    std::cout << "Overall QPS: " << std::fixed << std::setprecision(1) << overall_qps << " keys/s" << std::endl;

    if (args.cmd == "get") {
        std::cout << "Overall Avg Get Time: " << std::fixed << std::setprecision(2)
                  << (sum_get_time_us / args.thread_count / 1000.0) << " ms" << std::endl;
        std::cout << "Overall Avg H2D Time: " << std::fixed << std::setprecision(2)
                  << (sum_h2d_time_us / args.thread_count / 1000.0) << " ms" << std::endl;
    }
    std::cout << "=================================================" << std::endl;
}

// KPS mode multi-thread runner
void RunKpsMultiThread(const CmdArgs& args, const std::vector<std::pair<std::string, std::string>>& all_data) {
    int keys_per_thread = args.count / args.thread_count;

    auto barrier = std::make_shared<Barrier>(args.thread_count);
    std::vector<std::thread> threads;
    KpsOperationStats kps_stats;  // Shared statistics
    auto rate_limiter = std::make_shared<RateLimiter>(args.kps * 3, static_cast<int>(args.kps * 3));  // token rate = key rate * 3 ops

    std::cout << "[Main] Starting KPS mode with " << args.thread_count << " threads..." << std::endl;
    std::cout << "[Main] Target KPS: " << args.kps << ", Batch size: " << args.batch << std::endl;
    std::cout << "[Main] Duration: " << args.duration << " seconds" << std::endl;

    // Start worker threads
    for (int tid = 0; tid < args.thread_count; ++tid) {
        threads.emplace_back([tid, &args, &all_data, keys_per_thread, &kps_stats, rate_limiter, barrier]() {
            RemoteH2DTest test(args.localip, args.port, tid, args.gpu_id, args.verify_data, args.client_options);
            if (!test.Init()) {
                return;
            }
            test.SetBarrier(barrier);

            int start_idx = tid * keys_per_thread;
            int end_idx = start_idx + keys_per_thread;
            test.SetSharedData(all_data, start_idx, end_idx);

            test.RunKpsMode(args.batch, args.kps, kps_stats, rate_limiter, barrier);
        });
    }

    // Wait for all threads to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Start KPS mode
    g_kps_running = true;
    auto main_start = std::chrono::steady_clock::now();

    // Statistics printing thread
    std::thread stats_thread([&kps_stats, &args, &main_start]() {
        int64_t last_ops = 0;
        int64_t last_set_ops = 0;
        int64_t last_get_ops = 0;
        int64_t last_del_ops = 0;
        auto last_print_time = main_start;

        while (!g_kps_stop.load()) {
            std::this_thread::sleep_for(std::chrono::seconds(5));

            auto now = std::chrono::steady_clock::now();
            double interval_s = std::chrono::duration_cast<std::chrono::microseconds>(now - last_print_time).count() / 1000000.0;
            double total_s = std::chrono::duration_cast<std::chrono::microseconds>(now - main_start).count() / 1000000.0;

            if (interval_s <= 0) continue;

            int64_t current_ops = kps_stats.total_ops.load();
            int64_t current_set_ops = kps_stats.set_ops.load();
            int64_t current_get_ops = kps_stats.get_ops.load();
            int64_t current_del_ops = kps_stats.del_ops.load();

            int64_t interval_ops = current_ops - last_ops;
            double interval_kps = interval_ops / interval_s;

            std::cout << std::endl;
            std::cout << "=================== [" << std::fixed << std::setprecision(1) << total_s << "s] ===================" << std::endl;
            std::cout << "[Stats] Interval KPS: " << std::fixed << std::setprecision(1) << interval_kps << " ops/s" << std::endl;
            std::cout << "[Stats] Total ops: " << current_ops << " (set: " << current_set_ops
                      << ", get: " << current_get_ops << ", del: " << current_del_ops << ")" << std::endl;

            // Print latency percentiles for the interval
            std::vector<double> set_lats = kps_stats.GetSetLatencies();
            std::vector<double> get_lats = kps_stats.GetGetLatencies();
            std::vector<double> del_lats = kps_stats.GetDelLatencies();

            std::cout << std::fixed << std::setprecision(2);
            if (!set_lats.empty()) {
                std::cout << "[Stats] Set latency (us): P50=" << CalculatePercentile(set_lats, 50.0)
                          << ", P90=" << CalculatePercentile(set_lats, 90.0)
                          << ", P95=" << CalculatePercentile(set_lats, 95.0)
                          << ", P99=" << CalculatePercentile(set_lats, 99.0) << std::endl;
            }
            if (!get_lats.empty()) {
                std::cout << "[Stats] Get latency (us): P50=" << CalculatePercentile(get_lats, 50.0)
                          << ", P90=" << CalculatePercentile(get_lats, 90.0)
                          << ", P95=" << CalculatePercentile(get_lats, 95.0)
                          << ", P99=" << CalculatePercentile(get_lats, 99.0) << std::endl;
            }
            if (!del_lats.empty()) {
                std::cout << "[Stats] Del latency (us): P50=" << CalculatePercentile(del_lats, 50.0)
                          << ", P90=" << CalculatePercentile(del_lats, 90.0)
                          << ", P95=" << CalculatePercentile(del_lats, 95.0)
                          << ", P99=" << CalculatePercentile(del_lats, 99.0) << std::endl;
            }
            std::cout << "===========================================================" << std::endl;

            last_ops = current_ops;
            last_set_ops = current_set_ops;
            last_get_ops = current_get_ops;
            last_del_ops = current_del_ops;
            last_print_time = now;
        }
    });

    // Wait for duration
    std::this_thread::sleep_for(std::chrono::seconds(args.duration));

    // Stop KPS mode
    g_kps_stop = true;
    g_kps_running = false;

    // Wait for all threads to complete
    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }
    if (stats_thread.joinable()) stats_thread.join();

    auto main_end = std::chrono::steady_clock::now();
    double total_time_s = std::chrono::duration_cast<std::chrono::microseconds>(main_end - main_start).count() / 1000000.0;

    // Print final summary
    std::cout << std::endl;
    std::cout << "==================== Final Summary ====================" << std::endl;
    std::cout << "Target KPS: " << args.kps << std::endl;
    std::cout << "Duration: " << total_time_s << " s" << std::endl;
    std::cout << "Threads: " << args.thread_count << std::endl;
    std::cout << "Batch size: " << args.batch << std::endl;
    std::cout << std::endl;

    PrintKpsStats(kps_stats, total_time_s, "");
    std::cout << "=======================================================" << std::endl;
}

// KPS mode multi-process runner
void RunKpsMultiProcess(const CmdArgs& args, const std::vector<std::pair<std::string, std::string>>& all_data) {
    int process_count = args.process_count;
    int keys_per_process = args.count / process_count;

    std::cout << "[Main] Starting KPS mode with " << process_count << " processes..." << std::endl;
    std::cout << "[Main] Target KPS: " << args.kps << ", Batch size: " << args.batch << std::endl;
    std::cout << "[Main] Duration: " << args.duration << " seconds" << std::endl;

    // Create a shared memory region for inter-process communication
    // Using file-based approach for simplicity
    std::string stop_file = "/tmp/rh2d_kps_stop_" + std::to_string(getpid()) + ".tmp";
    std::string running_file = "/tmp/rh2d_kps_running_" + std::to_string(getpid()) + ".tmp";
    std::string stats_dir = "/tmp/rh2d_kps_stats_" + std::to_string(getpid());
    mkdir(stats_dir.c_str(), 0755);

    // Reset stop signal
    std::ofstream(stop_file).close();
    std::ofstream(running_file) << "1" << std::endl;

    std::vector<pid_t> pids(process_count);

    auto main_start = std::chrono::steady_clock::now();

    for (int pid_idx = 0; pid_idx < process_count; ++pid_idx) {
        pid_t pid = fork();

        if (pid < 0) {
            std::cerr << "Fork failed for process " << pid_idx << std::endl;
            continue;
        }

        if (pid == 0) {
            // Child process
            RemoteH2DTest test(args.localip, args.port, pid_idx, args.gpu_id, args.verify_data,
                               args.client_options);
            if (!test.Init()) {
                exit(1);
            }

#ifndef USE_PIPLN_MOCK
            cudaError_t err;
            if ((err = cudaSetDevice(args.gpu_id)) != cudaSuccess) {
                std::cerr << "[P" << pid_idx << "] cudaSetDevice failed: " << cudaGetErrorString(err) << std::endl;
                exit(1);
            }

            // Pre-allocate CUDA memory for H2D operations
            // Each batch element needs independent memory to avoid data corruption
            std::vector<void*> preallocated_ptrs;
            size_t max_size = 0;
            for (int i = 0; i < keys_per_process; ++i) {
                max_size = std::max(max_size, all_data[pid_idx * keys_per_process + i].second.size());
            }
            // Allocate batch_size independent buffers
            for (int i = 0; i < args.batch; ++i) {
                void* dev_ptr = nullptr;
                if (cudaMalloc(&dev_ptr, max_size) == cudaSuccess) {
                    preallocated_ptrs.push_back(dev_ptr);
                }
            }
#endif

            int start_idx = pid_idx * keys_per_process;
            int end_idx = start_idx + keys_per_process;

            // Local rate limiter for this process
            double process_kps = args.kps / process_count;
            RateLimiter rate_limiter(process_kps * 3, static_cast<int>(process_kps * 3));  // token rate = key rate * 3 ops

            std::random_device rd;
            std::mt19937 rng(rd());
            int total_keys = end_idx - start_idx;
            std::uniform_int_distribution<int> key_dist(0, total_keys - args.batch);

            // Wait for running signal
            while (!std::ifstream(running_file).good()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }

            KpsOperationStats local_stats;
            auto last_report = std::chrono::steady_clock::now();

            while (true) {
                // Check stop signal
                std::ifstream stop_ifs(stop_file);
                std::string stop_content;
                std::getline(stop_ifs, stop_content);
                if (stop_content == "stop") {
                    break;
                }

                // Apply rate limiting
                int64_t wait_us = rate_limiter.Acquire(args.batch * 3);
                if (wait_us > 0) {
                    std::this_thread::sleep_for(std::chrono::microseconds(wait_us));
                }

                // Randomly select starting index for batch
                int start = key_dist(rng);

                // ========== Batch Set ==========
                std::vector<std::string> keys;
                std::vector<std::string> values;
                for (int i = 0; i < args.batch; ++i) {
                    keys.push_back(all_data[start_idx + start + i].first);
                    values.push_back(all_data[start_idx + start + i].second);
                }

                auto set_start = std::chrono::high_resolution_clock::now();
                std::vector<std::string> setFailedKeys;
                Status rc;
                for (size_t i = 0; i < keys.size(); ++i) {
                    Status s = test.GetClient()->Set(keys[i], values[i]);
                    if (s.IsError()) {
                        setFailedKeys.push_back(keys[i]);
                        rc = s;  // Keep last error
                    }
                }
                auto set_end = std::chrono::high_resolution_clock::now();
                double set_us = std::chrono::duration_cast<std::chrono::microseconds>(set_end - set_start).count();
                local_stats.AddSetLatency(set_us);
                local_stats.set_ops += args.batch;
                local_stats.total_ops += args.batch;
                if (rc.IsError() || !setFailedKeys.empty()) {
                    local_stats.failed_ops += args.batch;
                    std::cerr << "[P" << pid_idx << "] Set failed: " << rc.GetMsg()
                              << ", failed keys count: " << setFailedKeys.size() << std::endl;
                    if (!setFailedKeys.empty()) {
                        std::cerr << "[P" << pid_idx << "] Set failed keys: ";
                        for (const auto& key : setFailedKeys) {
                            std::cerr << key << " ";
                        }
                        std::cerr << std::endl;
                    }
                }

                // ========== Batch MGetH2D ==========
                std::vector<Blob> devShmChunks;
                std::vector<std::string> outFailedKeys;

#ifndef USE_PIPLN_MOCK
                for (int i = 0; i < args.batch; ++i) {
                    void* ptr = nullptr;
                    size_t size = all_data[start_idx + start + i].second.size();
                    if (!preallocated_ptrs.empty()) {
                        // Use pre-allocated independent buffer for each key
                        ptr = preallocated_ptrs[i];
                    } else {
                        // Allocate on-the-fly if no pre-allocated buffers
                        cudaMalloc(&ptr, size);
                    }
                    devShmChunks.push_back(Blob{ptr, static_cast<uint64_t>(size)});
                }
#endif

                auto rh2d_start = std::chrono::high_resolution_clock::now();
                rc = test.GetClient()->MGetH2D(keys, devShmChunks, outFailedKeys);
                auto rh2d_end = std::chrono::high_resolution_clock::now();
                double rh2d_us = std::chrono::duration_cast<std::chrono::microseconds>(rh2d_end - rh2d_start).count();
                local_stats.AddGetLatency(rh2d_us);
                local_stats.get_ops += args.batch;
                local_stats.total_ops += args.batch;
                if (rc.IsError() || !outFailedKeys.empty()) {
                    local_stats.failed_ops += args.batch;
                    std::cerr << "[P" << pid_idx << "] MGetH2D failed: " << rc.GetMsg()
                              << ", failed keys count: " << outFailedKeys.size() << std::endl;
                    if (!outFailedKeys.empty()) {
                        std::cerr << "[P" << pid_idx << "] MGetH2D failed keys: ";
                        for (const auto& key : outFailedKeys) {
                            std::cerr << key << " ";
                        }
                        std::cerr << std::endl;
                    }
                }

#ifndef USE_PIPLN_MOCK
                // Free per-batch CUDA memory if not pre-allocated
                if (preallocated_ptrs.empty()) {
                    for (auto& chunk : devShmChunks) {
                        if (chunk.pointer) cudaFree(chunk.pointer);
                    }
                }
                // Note: pre-allocated buffers are freed at process exit
#endif

                // ========== Batch Delete ==========
                auto del_start = std::chrono::high_resolution_clock::now();
                std::vector<std::string> delFailedKeys;
                for (const auto& key : keys) {
                    Status s = test.GetClient()->Del(key);
                    if (s.IsError()) {
                        delFailedKeys.push_back(key);
                        rc = s;  // Keep last error
                    }
                }
                auto del_end = std::chrono::high_resolution_clock::now();
                double del_us = std::chrono::duration_cast<std::chrono::microseconds>(del_end - del_start).count();
                local_stats.AddDelLatency(del_us);
                local_stats.del_ops += args.batch;
                local_stats.total_ops += args.batch;
                if (rc.IsError() || !delFailedKeys.empty()) {
                    local_stats.failed_ops += args.batch;
                    std::cerr << "[P" << pid_idx << "] Del failed: " << rc.GetMsg()
                              << ", failed keys count: " << delFailedKeys.size() << std::endl;
                    if (!delFailedKeys.empty()) {
                        std::cerr << "[P" << pid_idx << "] Del failed keys: ";
                        for (const auto& key : delFailedKeys) {
                            std::cerr << key << " ";
                        }
                        std::cerr << std::endl;
                    }
                }

                // Report stats periodically
                auto now = std::chrono::steady_clock::now();
                if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report).count() >= 5) {
                    std::string stats_file = stats_dir + "/stats_" + std::to_string(pid_idx) + ".tmp";
                    std::ofstream ofs(stats_file, std::ios::trunc);
                    if (ofs) {
                        ofs << local_stats.total_ops.load() << "\n";
                        ofs << local_stats.set_ops.load() << "\n";
                        ofs << local_stats.get_ops.load() << "\n";
                        ofs << local_stats.del_ops.load() << "\n";
                        ofs << local_stats.failed_ops.load() << "\n";
                        ofs.close();
                    }
                    last_report = now;
                }
            }

#ifndef USE_PIPLN_MOCK
            // Free pre-allocated CUDA memory
            for (auto ptr : preallocated_ptrs) {
                if (ptr) cudaFree(ptr);
            }
#endif

            // Write final stats
            std::string final_file = stats_dir + "/final_" + std::to_string(pid_idx) + ".tmp";
            std::ofstream ofs(final_file);
            if (ofs) {
                ofs << local_stats.total_ops.load() << "\n";
                ofs << local_stats.set_ops.load() << "\n";
                ofs << local_stats.get_ops.load() << "\n";
                ofs << local_stats.del_ops.load() << "\n";
                ofs << local_stats.failed_ops.load() << "\n";
                // Write latencies
                std::vector<double> set_lats = local_stats.GetSetLatencies();
                std::vector<double> get_lats = local_stats.GetGetLatencies();
                std::vector<double> del_lats = local_stats.GetDelLatencies();
                ofs << set_lats.size() << "\n";
                for (const auto& lat : set_lats) ofs << lat << "\n";
                ofs << get_lats.size() << "\n";
                for (const auto& lat : get_lats) ofs << lat << "\n";
                ofs << del_lats.size() << "\n";
                for (const auto& lat : del_lats) ofs << lat << "\n";
                ofs.close();
            }

            exit(0);
        } else {
            pids[pid_idx] = pid;
        }
    }

    // Wait for all processes to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Statistics printing loop in parent
    int64_t last_ops = 0;
    auto last_print_time = main_start;

    for (int sec = 0; sec < args.duration; sec += 5) {
        std::this_thread::sleep_for(std::chrono::seconds(5));

        auto now = std::chrono::steady_clock::now();
        double interval_s = std::chrono::duration_cast<std::chrono::microseconds>(now - last_print_time).count() / 1000000.0;
        double total_s = std::chrono::duration_cast<std::chrono::microseconds>(now - main_start).count() / 1000000.0;

        // Collect stats from all processes
        int64_t current_ops = 0;
        int64_t current_set_ops = 0;
        int64_t current_get_ops = 0;
        int64_t current_del_ops = 0;

        for (int pid_idx = 0; pid_idx < process_count; ++pid_idx) {
            std::string stats_file = stats_dir + "/stats_" + std::to_string(pid_idx) + ".tmp";
            std::ifstream ifs(stats_file);
            if (ifs) {
                int64_t ops, set_ops, get_ops, del_ops, failed_ops;
                ifs >> ops >> set_ops >> get_ops >> del_ops >> failed_ops;
                current_ops += ops;
                current_set_ops += set_ops;
                current_get_ops += get_ops;
                current_del_ops += del_ops;
                ifs.close();
            }
        }

        int64_t interval_ops = current_ops - last_ops;
        double interval_kps = interval_ops / interval_s;

        std::cout << std::endl;
        std::cout << "=================== [" << std::fixed << std::setprecision(1) << total_s << "s] ===================" << std::endl;
        std::cout << "[Stats] Interval KPS: " << std::fixed << std::setprecision(1) << interval_kps << " ops/s" << std::endl;
        std::cout << "[Stats] Total ops: " << current_ops << " (set: " << current_set_ops
                  << ", get: " << current_get_ops << ", del: " << current_del_ops << ")" << std::endl;
        std::cout << "===========================================================" << std::endl;

        last_ops = current_ops;
        last_print_time = now;
    }

    // Stop all processes
    std::ofstream stop_ofs(stop_file);
    stop_ofs << "stop" << std::endl;
    stop_ofs.close();

    // Wait for all processes
    for (int i = 0; i < process_count; ++i) {
        int status;
        waitpid(pids[i], &status, 0);
    }

    auto main_end = std::chrono::steady_clock::now();
    double total_time_s = std::chrono::duration_cast<std::chrono::microseconds>(main_end - main_start).count() / 1000000.0;

    // Collect final stats
    KpsOperationStats final_stats;
    for (int pid_idx = 0; pid_idx < process_count; ++pid_idx) {
        std::string final_file = stats_dir + "/final_" + std::to_string(pid_idx) + ".tmp";
        std::ifstream ifs(final_file);
        if (ifs) {
            int64_t ops, set_ops, get_ops, del_ops, failed_ops;
            ifs >> ops >> set_ops >> get_ops >> del_ops >> failed_ops;
            final_stats.total_ops += ops;
            final_stats.set_ops += set_ops;
            final_stats.get_ops += get_ops;
            final_stats.del_ops += del_ops;
            final_stats.failed_ops += failed_ops;

            // Read latencies
            size_t count;
            double lat;

            ifs >> count;
            for (size_t i = 0; i < count; ++i) {
                ifs >> lat;
                final_stats.AddSetLatency(lat);
            }
            ifs >> count;
            for (size_t i = 0; i < count; ++i) {
                ifs >> lat;
                final_stats.AddGetLatency(lat);
            }
            ifs >> count;
            for (size_t i = 0; i < count; ++i) {
                ifs >> lat;
                final_stats.AddDelLatency(lat);
            }
            ifs.close();
        }
    }

    // Print final summary
    std::cout << std::endl;
    std::cout << "==================== Final Summary ====================" << std::endl;
    std::cout << "Target KPS: " << args.kps << std::endl;
    std::cout << "Duration: " << total_time_s << " s" << std::endl;
    std::cout << "Processes: " << process_count << std::endl;
    std::cout << "Batch size: " << args.batch << std::endl;
    std::cout << std::endl;

    PrintKpsStats(final_stats, total_time_s, "");
    std::cout << "=======================================================" << std::endl;

    // Cleanup
    unlink(stop_file.c_str());
    unlink(running_file.c_str());
    for (int pid_idx = 0; pid_idx < process_count; ++pid_idx) {
        std::string stats_file = stats_dir + "/stats_" + std::to_string(pid_idx) + ".tmp";
        std::string final_file = stats_dir + "/final_" + std::to_string(pid_idx) + ".tmp";
        unlink(stats_file.c_str());
        unlink(final_file.c_str());
    }
    rmdir(stats_dir.c_str());
}

int main(int argc, char* argv[]) {
    CmdArgs args = ParseArgs(argc, argv);

    if (args.help) {
        PrintUsage(argv[0]);
        return 0;
    }

    if (args.cmd.empty()) {
        std::cerr << "Error: Command is required (rh2d, get, or kps)" << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }

    if (args.cmd != "rh2d" && args.cmd != "get" && args.cmd != "kps") {
        std::cerr << "Error: Unknown command '" << args.cmd << "'" << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }

    if (args.remoteip.empty()) {
        std::cerr << "Error: --remoteip is required" << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }

    if (args.localip.empty()) {
        args.localip = args.remoteip;
    }

    if (args.count <= 0 || args.batch <= 0) {
        std::cerr << "Error: count and batch must be positive integers" << std::endl;
        return 1;
    }

    // KPS mode specific validation
    if (args.cmd == "kps") {
        if (args.kps <= 0) {
            std::cerr << "Error: --kps must be positive for kps mode" << std::endl;
            return 1;
        }
        if (args.duration <= 0) {
            std::cerr << "Error: --duration must be positive for kps mode" << std::endl;
            return 1;
        }
        if (args.count < args.batch) {
            std::cerr << "Error: count must be >= batch for kps mode" << std::endl;
            return 1;
        }
    }

    // Determine execution mode
    int parallel_count = args.process_count > 0 ? args.process_count : args.thread_count;

    if (args.cmd != "kps") {
        // Original validation for non-kps modes
        if (args.count % args.batch != 0) {
            std::cerr << "Error: count must be divisible by batch" << std::endl;
            return 1;
        }

        if (args.count % parallel_count != 0) {
            std::cerr << "Error: count must be divisible by " << (args.process_count > 0 ? "process" : "thread") << std::endl;
            return 1;
        }

        if (args.count % (args.batch * parallel_count) != 0) {
            std::cerr << "Error: count must be divisible by (batch * " << (args.process_count > 0 ? "process" : "thread") << ")" << std::endl;
            return 1;
        }
    }

    std::cout << "[Main] Command: " << args.cmd << std::endl;
    std::cout << "[Main] Mode: " << (args.process_count > 0 ? "Multi-process" : "Multi-thread") << std::endl;
    std::cout << "[Main] Count: " << args.count << ", Batch: " << args.batch
              << ", " << (args.process_count > 0 ? "Processes" : "Threads") << ": " << parallel_count << std::endl;
    if (args.cmd == "kps") {
        std::cout << "[Main] Target KPS: " << args.kps << ", Duration: " << args.duration << "s" << std::endl;
    }
    std::cout << "[Main] RemoteIP: " << args.remoteip << ", LocalIP: " << args.localip
              << ", Port: " << args.port << ", GPU: " << args.gpu_id << std::endl;
    std::cout << "[Main] Verify Data: " << (args.verify_data ? "Yes" : "No")
              << ", Use User Stream: " << (args.use_user_stream ? "Yes" : "No") << std::endl;
    std::cout << "[Main] Enable Local Cache: "
              << (args.client_options.enable_local_cache ? "Yes" : "No")
              << ", Client-direct RH2D: "
              << (args.client_options.enable_client_direct_rh2d ? "Yes" : "No") << std::endl;
    std::cout << "[Main] Client-direct Threads: " << args.client_options.client_direct_thread_num
              << ", Fast Transport Memory: " << args.client_options.fast_transport_mem_size << std::endl;

    // Parse value size configuration
    std::vector<SizeConfig> size_configs;
    if (!args.valuesize_config.empty()) {
        // Parse new format: size1:num1,size2:num2,size3
        std::vector<std::string> parts = SplitString(args.valuesize_config, ',');
        for (const auto& part : parts) {
            std::vector<std::string> size_num = SplitString(part, ':');
            if (size_num.empty()) continue;

            size_t size = std::stoull(size_num[0]);
            int count = -1;  // -1 means remaining

            if (size_num.size() > 1) {
                count = std::stoi(size_num[1]);
            }

            size_configs.push_back({size, count});
        }
    } else {
        // Legacy: single size
        size_configs.push_back({args.value_size, -1});
    }

    // Print size configuration
    std::cout << "[Main] Value Size Config: ";
    for (size_t i = 0; i < size_configs.size(); ++i) {
        if (i > 0) std::cout << ", ";
        std::cout << size_configs[i].size << " bytes";
        if (size_configs[i].count > 0) {
            std::cout << " x " << size_configs[i].count;
        } else {
            std::cout << " (remaining)";
        }
    }
    std::cout << std::endl;

    // Generate data based on size configuration
    std::cout << "[Main] Generating " << args.count << " random key-value pairs..." << std::endl;
    std::vector<std::pair<std::string, std::string>> all_data;
    all_data.reserve(args.count);
    int progress_interval = std::max(1, args.count / 20); // Print progress every 5%

    int num_batches = args.count / args.batch;
    for (int batch_idx = 0; batch_idx < num_batches; ++batch_idx) {
        // Calculate how many keys for each size in this batch
        std::vector<std::pair<size_t, int>> batch_sizes;
        int allocated = 0;

        for (const auto& config : size_configs) {
            if (config.count == -1) {
                // Remaining keys in batch
                int remaining = args.batch - allocated;
                if (remaining > 0) {
                    batch_sizes.push_back({config.size, remaining});
                    allocated = args.batch;
                }
            } else {
                int count = std::min(config.count, args.batch - allocated);
                if (count > 0) {
                    batch_sizes.push_back({config.size, count});
                    allocated += count;
                }
            }
            if (allocated >= args.batch) break;
        }

        // Generate data for this batch
        for (const auto& sc : batch_sizes) {
            size_t size = sc.first;
            int count = sc.second;
            for (int j = 0; j < count; ++j) {
                std::string key = "key_" + std::to_string(all_data.size()) + "_" + GenerateRandomString(8);
                std::string value = GenerateRandomString(size);
                all_data.emplace_back(key, value);

                if (static_cast<int>(all_data.size()) % progress_interval == 0 ||
                    static_cast<int>(all_data.size()) == args.count) {
                    int progress = static_cast<int>(all_data.size()) * 100 / args.count;
                    std::cout << "[Main] Generate progress: " << std::setw(3) << progress << "% ("
                              << all_data.size() << "/" << args.count << ")" << std::endl;
                }
            }
        }
    }
    std::cout << "[Main] Data generation completed." << std::endl;

    // For kps mode, run the KPS test directly without pre-setting data
    if (args.cmd == "kps") {
        if (args.process_count > 0) {
            RunKpsMultiProcess(args, all_data);
        } else {
            RunKpsMultiThread(args, all_data);
        }
        return 0;
    }

    // For rh2d/get mode, pre-set data to remote server
    std::cout << "[Main] Setting data to remote server " << args.remoteip << ":" << args.port << "..." << std::endl;
    {
        ConnectOptions connectOptions;
        connectOptions.host = args.remoteip;
        connectOptions.port = args.port;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(args.gpu_id);

        KVClient remoteClient(connectOptions);
        Status rc = remoteClient.Init();
        if (rc.IsError()) {
            std::cerr << "[Main] Failed to connect to remote server: " << rc.GetMsg() << std::endl;
            return 1;
        }

        int set_count = 0;
        int set_failed = 0;
        int set_progress_interval = std::max(1, args.count / 20); // Print progress every 5%
        for (const auto& kv : all_data) {
            rc = remoteClient.Set(kv.first, kv.second);
            if (rc.IsError()) {
                set_failed++;
                if (set_failed <= 5) {
                    std::cerr << "[Main] Set failed for key " << kv.first << ": " << rc.GetMsg() << std::endl;
                }
            } else {
                set_count++;
            }
            if (set_count % set_progress_interval == 0 || set_count == args.count) {
                int progress = set_count * 100 / args.count;
                std::cout << "[Main] Set progress: " << std::setw(3) << progress << "% ("
                          << set_count << "/" << args.count << ")" << std::endl;
            }
        }
        std::cout << "[Main] Set completed. Success: " << set_count << ", Failed: " << set_failed << std::endl;
    }

    // Run in either multi-process or multi-thread mode
    if (args.process_count > 0) {
        RunMultiProcess(args, all_data);
    } else {
        RunMultiThread(args, all_data);
    }

    // Delete all keys after test completion
    std::cout << "[Main] Deleting all keys from remote server..." << std::endl;
    {
        ConnectOptions connectOptions;
        connectOptions.host = args.remoteip;
        connectOptions.port = args.port;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(args.gpu_id);

        KVClient remoteClient(connectOptions);
        Status rc = remoteClient.Init();
        if (rc.IsError()) {
            std::cerr << "[Main] Failed to connect to remote server for deletion: " << rc.GetMsg() << std::endl;
            return 1;
        }

        int del_count = 0;
        int del_failed = 0;
        int del_progress_interval = std::max(1, args.count / 20); // Print progress every 5%
        for (const auto& kv : all_data) {
            rc = remoteClient.Del(kv.first);
            if (rc.IsError()) {
                del_failed++;
                if (del_failed <= 5) {
                    std::cerr << "[Main] Del failed for key " << kv.first << ": " << rc.GetMsg() << std::endl;
                }
            } else {
                del_count++;
            }
            if (del_count % del_progress_interval == 0 || del_count == args.count) {
                int progress = del_count * 100 / args.count;
                std::cout << "[Main] Del progress: " << std::setw(3) << progress << "% ("
                          << del_count << "/" << args.count << ")" << std::endl;
            }
        }
        std::cout << "[Main] Del completed. Success: " << del_count << ", Failed: " << del_failed << std::endl;
    }

    return 0;
}
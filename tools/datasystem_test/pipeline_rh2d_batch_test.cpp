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

// Size configuration for batch
struct SizeConfig {
    size_t size;
    int count;  // -1 means remaining in batch
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
    RemoteH2DTest(const std::string& host, int port, int thread_id, int gpu_id, bool verify_data = true)
        : thread_id_(thread_id), gpu_id_(gpu_id), host_(host), port_(port), verify_data_(verify_data) {
        client_ = nullptr;
    }

    bool Init() {
        ConnectOptions connectOptions;
        connectOptions.host = host_;
        connectOptions.port = port_;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(gpu_id_);

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

            if (ret == datasystem::Status::OK()) {
                TLOG(thread_id_, "Round " << round << " MGetH2D success! Time: " << duration_us << " us"
                     << (use_user_stream ? " (with user stream)" : ""));
            } else {
                TLOG(thread_id_, "Round " << round << " MGetH2D completed. Failed keys: " << outFailedKeys.size());
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
};

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <command> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  rh2d : Set data to remote, then MGetH2D from local" << std::endl;
    std::cout << "  get  : Set data to remote, then Get and copy to CUDA" << std::endl;
    std::cout << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --count=N         Total keys (default: 100)" << std::endl;
    std::cout << "  --batch=N         Keys per batch (default: 10)" << std::endl;
    std::cout << "  --thread=N        Threads (default: 1)" << std::endl;
    std::cout << "  --process=N       Processes (default: 0, 0=multi-thread)" << std::endl;
    std::cout << "  --valuesize=CFG   Value size config (default: 8388608)" << std::endl;
    std::cout << "                    Format: size1:num1,size2:num2,size3" << std::endl;
    std::cout << "                    Last size without :num = remaining" << std::endl;
    std::cout << "  --remoteip=IP     Remote server IP (required)" << std::endl;
    std::cout << "  --localip=IP      Local IP (default: same as remoteip)" << std::endl;
    std::cout << "  --port=N          Server port (default: 18481)" << std::endl;
    std::cout << "  --gpu_id=N        GPU device ID (default: 0)" << std::endl;
    std::cout << "  --verify=Y/N      Verify data (default: Y)" << std::endl;
    std::cout << "  --use_user_stream=Y/N  Use new MGetH2D interface (default: N)" << std::endl;
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
}

struct CmdArgs {
    std::string cmd;
    int count = 100;
    int batch = 10;
    int thread_count = 1;
    int process_count = 0;
    std::string valuesize_config;  // Format: "size1:num1,size2:num2,size3"
    size_t value_size = 8388608;   // Legacy single size (used if valuesize_config empty)
    std::string remoteip;
    std::string localip;
    int port = 18481;
    int gpu_id = 0;
    bool verify_data = true;
    bool use_user_stream = false;
    bool help = false;
};

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
            if (key == "count") args.count = std::stoi(value);
            else if (key == "batch") args.batch = std::stoi(value);
            else if (key == "thread") args.thread_count = std::stoi(value);
            else if (key == "process") args.process_count = std::stoi(value);
            else if (key == "valuesize") args.valuesize_config = value;  // Support new format
            else if (key == "remoteip") args.remoteip = value;
            else if (key == "localip") args.localip = value;
            else if (key == "port") args.port = std::stoi(value);
            else if (key == "gpu_id") args.gpu_id = std::stoi(value);
            else if (key == "verify") {
                std::transform(value.begin(), value.end(), value.begin(), ::tolower);
                args.verify_data = (value == "y" || value == "yes" || value == "1" || value == "true");
            }
            else if (key == "use_user_stream") {
                std::transform(value.begin(), value.end(), value.begin(), ::tolower);
                args.use_user_stream = (value == "y" || value == "yes" || value == "1" || value == "true");
            }
            continue;
        }

        if (arg.size() >= 3 && arg[0] == '-' && arg[1] == '-') {
            key = arg.substr(2);
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                value = argv[++i];
                if (key == "count") args.count = std::stoi(value);
                else if (key == "batch") args.batch = std::stoi(value);
                else if (key == "thread") args.thread_count = std::stoi(value);
                else if (key == "process") args.process_count = std::stoi(value);
                else if (key == "valuesize") args.valuesize_config = value;  // Support new format
                else if (key == "remoteip") args.remoteip = value;
                else if (key == "localip") args.localip = value;
                else if (key == "port") args.port = std::stoi(value);
                else if (key == "gpu_id") args.gpu_id = std::stoi(value);
                else if (key == "verify") {
                    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
                    args.verify_data = (value == "y" || value == "yes" || value == "1" || value == "true");
                }
                else if (key == "use_user_stream") {
                    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
                    args.use_user_stream = (value == "y" || value == "yes" || value == "1" || value == "true");
                }
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
    std::cout << prefix << "  P99:     " << std::setw(10) << (stats.GetP99() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  P99.99:  " << std::setw(10) << (stats.GetP9999() / 1000.0) << " ms" << std::endl;
    std::cout << prefix << "  MAX:     " << std::setw(10) << (stats.GetMax() / 1000.0) << " ms" << std::endl;
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
            RemoteH2DTest test(args.localip, args.port, pid_idx, args.gpu_id, args.verify_data);
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
            RemoteH2DTest test(args.localip, args.port, tid, args.gpu_id, args.verify_data);
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

int main(int argc, char* argv[]) {
    CmdArgs args = ParseArgs(argc, argv);

    if (args.help) {
        PrintUsage(argv[0]);
        return 0;
    }

    if (args.cmd.empty()) {
        std::cerr << "Error: Command is required (rh2d or get)" << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }

    if (args.cmd != "rh2d" && args.cmd != "get") {
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

    // Determine execution mode
    int parallel_count = args.process_count > 0 ? args.process_count : args.thread_count;

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

    std::cout << "[Main] Command: " << args.cmd << std::endl;
    std::cout << "[Main] Mode: " << (args.process_count > 0 ? "Multi-process" : "Multi-thread") << std::endl;
    std::cout << "[Main] Count: " << args.count << ", Batch: " << args.batch
              << ", " << (args.process_count > 0 ? "Processes" : "Threads") << ": " << parallel_count << std::endl;
    std::cout << "[Main] RemoteIP: " << args.remoteip << ", LocalIP: " << args.localip
              << ", Port: " << args.port << ", GPU: " << args.gpu_id << std::endl;
    std::cout << "[Main] Verify Data: " << (args.verify_data ? "Yes" : "No")
              << ", Use User Stream: " << (args.use_user_stream ? "Yes" : "No") << std::endl;

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

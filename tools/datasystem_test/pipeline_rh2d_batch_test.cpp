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
#include <cuda_runtime.h>
#include <chrono>
#include <algorithm>
#include <thread>
#include <mutex>
#include <memory>
#include <condition_variable>
#include <atomic>
#include <random>

using namespace datasystem;

std::mutex print_mutex;

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
    Barrier(int count) : total_(count), count_(count), generation_(0) {}

    void Wait() {
        std::unique_lock<std::mutex> lock(mutex_);
        int gen = generation_;
        if (--count_ == 0) {
            generation_++;
            count_ = total_;
            cv_.notify_all();
        } else {
            cv_.wait(lock, [this, gen] { return gen != generation_; });
        }
    }

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int total_;
    int count_;
    int generation_;
};

struct Stats {
    double total_time_us;
    int batch_count;
    int key_count;
    double get_time_us;
    double h2d_time_us;
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
    RemoteH2DTest(const std::string& host, int port, int thread_id, int gpu_id)
        : thread_id_(thread_id), gpu_id_(gpu_id), host_(host), port_(port) {
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

    void SetDataParams(size_t value_size) {
        value_size_ = (value_size > 0) ? value_size : 8388608;
    }

    void SetBarrier(std::shared_ptr<Barrier> barrier) { barrier_ = barrier; }

    void SetSharedData(const std::vector<std::pair<std::string, std::string>>& data,
                       int start_idx, int end_idx) {
        data_.clear();
        for (int i = start_idx; i < end_idx; ++i) {
            data_.push_back(data[i]);
        }
    }

    void RunRh2DBatch(int batch, Stats& stats) {
        if (barrier_) barrier_->Wait();

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

        for (int round = 0; round < num_batches; ++round) {
            std::vector<std::string> keys;
            std::vector<std::pair<void*, size_t>> devShmChunks;
            std::vector<std::string> outFailedKeys;

            int start = round * batch;
            int end = std::min((round + 1) * batch, total_keys);

            for (int i = start; i < end; ++i) {
                keys.push_back(data_[i].first);
                void* dev_ptr = nullptr;
                if ((err = cudaMalloc(&dev_ptr, data_[i].second.size())) != cudaSuccess) {
                    TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                    for (auto& chunk : devShmChunks) cudaFree(chunk.first);
                    return;
                }
                devShmChunks.push_back({dev_ptr, data_[i].second.size()});
            }

            auto start_time = std::chrono::high_resolution_clock::now();
            auto ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys, 6000000);
            auto end_time = std::chrono::high_resolution_clock::now();

            double duration_us = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time).count();
            stats.total_time_us += duration_us;

            if (ret == datasystem::Status::OK()) {
                TLOG(thread_id_, "Round " << round << " MGetH2D success! Time: " << duration_us << " us");
            } else {
                TLOG(thread_id_, "Round " << round << " MGetH2D completed. Failed keys: " << outFailedKeys.size());
            }

            if (outFailedKeys.empty()) {
                CheckDataBatch(start, devShmChunks);
            }

            for (auto& chunk : devShmChunks) {
                cudaFree(chunk.first);
            }
        }
    }

    void RunGetBatch(int batch, Stats& stats) {
        if (barrier_) barrier_->Wait();

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

        for (int round = 0; round < num_batches; ++round) {
            std::vector<std::string> keys;
            std::vector<void*> dev_ptrs;
            std::vector<std::string> values;

            int start = round * batch;
            int end = std::min((round + 1) * batch, total_keys);

            for (int i = start; i < end; ++i) {
                keys.push_back(data_[i].first);
                void* dev_ptr = nullptr;
                if ((err = cudaMalloc(&dev_ptr, data_[i].second.size() + 1)) != cudaSuccess) {
                    TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                    for (auto ptr : dev_ptrs) cudaFree(ptr);
                    return;
                }
                dev_ptrs.push_back(dev_ptr);
            }

            auto get_start = std::chrono::high_resolution_clock::now();
            Status rc = client_->Get(keys, values, 6000000);
            auto get_end = std::chrono::high_resolution_clock::now();

            double get_duration_us = std::chrono::duration_cast<std::chrono::microseconds>(get_end - get_start).count();
            stats.get_time_us += get_duration_us;

            if (rc.IsError()) {
                TLOG(thread_id_, "Round " << round << " Get failed: " << rc.GetMsg());
                for (auto ptr : dev_ptrs) cudaFree(ptr);
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
            stats.h2d_time_us += h2d_duration_us;
            stats.total_time_us += get_duration_us + h2d_duration_us;

            TLOG(thread_id_, "Round " << round << " Get: " << get_duration_us << " us, H2D: " << h2d_duration_us << " us");

            CheckDataBatchHost(start, values);

            for (auto ptr : dev_ptrs) {
                cudaFree(ptr);
            }
        }
    }

private:
    void CheckDataBatch(int startIdx, const std::vector<std::pair<void*, size_t>>& devShmChunks) {
        bool is_failed = false;
        for (size_t i = 0; i < devShmChunks.size(); i++) {
            void* ptr = devShmChunks[i].first;
            size_t size = devShmChunks[i].second;
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
    }

    void CheckDataBatchHost(int startIdx, const std::vector<std::string>& values) {
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
    }

    std::unique_ptr<KVClient> client_;
    std::vector<std::pair<std::string, std::string>> data_;
    size_t value_size_ = 8388608;
    int thread_id_ = 0;
    int gpu_id_ = 0;
    std::string host_;
    int port_;
    std::shared_ptr<Barrier> barrier_;
};

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <command> [options]" << std::endl;
    std::cout << std::endl;
    std::cout << "Commands:" << std::endl;
    std::cout << "  rh2d    : Remote H2D test - Set data to remote, then MGetH2D from local" << std::endl;
    std::cout << "  get     : Get test - Set data to remote, then Get and copy to CUDA" << std::endl;
    std::cout << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --count=N       : Total number of keys (default: 100)" << std::endl;
    std::cout << "  --batch=N       : Keys per batch (default: 10)" << std::endl;
    std::cout << "  --thread=N      : Number of threads (default: 1)" << std::endl;
    std::cout << "  --valuesize=N   : Value size in bytes (default: 8388608)" << std::endl;
    std::cout << "  --remoteip=IP   : Remote server IP (required)" << std::endl;
    std::cout << "  --localip=IP    : Local server IP (default: same as remoteip)" << std::endl;
    std::cout << "  --port=N        : Server port (default: 18481)" << std::endl;
    std::cout << "  --gpu_id=N      : GPU device ID (default: 0)" << std::endl;
    std::cout << "  --help, -h      : Show this help message" << std::endl;
    std::cout << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << prog << " rh2d --count=100 --batch=10 --thread=4 --valuesize=1048576 --remoteip=192.168.1.100" << std::endl;
    std::cout << "  " << prog << " get --count=100 --batch=10 --thread=4 --valuesize=1048576 --remoteip=192.168.1.100 --localip=192.168.1.101" << std::endl;
}

struct CmdArgs {
    std::string cmd;
    int count = 100;
    int batch = 10;
    int thread_count = 1;
    size_t value_size = 8388608;
    std::string remoteip;
    std::string localip;
    int port = 18481;
    int gpu_id = 0;
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
            else if (key == "valuesize") args.value_size = std::stoull(value);
            else if (key == "remoteip") args.remoteip = value;
            else if (key == "localip") args.localip = value;
            else if (key == "port") args.port = std::stoi(value);
            else if (key == "gpu_id") args.gpu_id = std::stoi(value);
            continue;
        }

        if (arg.size() >= 3 && arg[0] == '-' && arg[1] == '-') {
            key = arg.substr(2);
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                value = argv[++i];
                if (key == "count") args.count = std::stoi(value);
                else if (key == "batch") args.batch = std::stoi(value);
                else if (key == "thread") args.thread_count = std::stoi(value);
                else if (key == "valuesize") args.value_size = std::stoull(value);
                else if (key == "remoteip") args.remoteip = value;
                else if (key == "localip") args.localip = value;
                else if (key == "port") args.port = std::stoi(value);
                else if (key == "gpu_id") args.gpu_id = std::stoi(value);
            }
        } else if (arg[0] != '-') {
            if (args.cmd.empty()) {
                args.cmd = arg;
            }
        }
    }

    return args;
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

    if (args.count <= 0 || args.batch <= 0 || args.thread_count <= 0) {
        std::cerr << "Error: count, batch, and thread must be positive integers" << std::endl;
        return 1;
    }

    if (args.count % args.batch != 0) {
        std::cerr << "Error: count must be divisible by batch" << std::endl;
        return 1;
    }

    if (args.count % args.thread_count != 0) {
        std::cerr << "Error: count must be divisible by thread" << std::endl;
        return 1;
    }

    if (args.count % (args.batch * args.thread_count) != 0) {
        std::cerr << "Error: count must be divisible by (batch * thread)" << std::endl;
        return 1;
    }

    std::cout << "[Main] Command: " << args.cmd << std::endl;
    std::cout << "[Main] Count: " << args.count << ", Batch: " << args.batch
              << ", Thread: " << args.thread_count << ", ValueSize: " << args.value_size << std::endl;
    std::cout << "[Main] RemoteIP: " << args.remoteip << ", LocalIP: " << args.localip
              << ", Port: " << args.port << ", GPU: " << args.gpu_id << std::endl;

    std::cout << "[Main] Generating " << args.count << " random key-value pairs..." << std::endl;
    std::vector<std::pair<std::string, std::string>> all_data;
    all_data.reserve(args.count);
    for (int i = 0; i < args.count; ++i) {
        std::string key = "key_" + std::to_string(i) + "_" + GenerateRandomString(8);
        std::string value = GenerateRandomString(args.value_size);
        all_data.emplace_back(key, value);
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
            if (set_count % 100 == 0) {
                std::cout << "[Main] Set progress: " << set_count << "/" << args.count << std::endl;
            }
        }
        std::cout << "[Main] Set completed. Success: " << set_count << ", Failed: " << set_failed << std::endl;
    }

    int keys_per_thread = args.count / args.thread_count;

    auto barrier = std::make_shared<Barrier>(args.thread_count);
    std::vector<std::thread> threads;
    std::vector<Stats> thread_stats(args.thread_count);

    std::cout << "[Main] Starting " << args.thread_count << " threads..." << std::endl;
    auto main_start = std::chrono::high_resolution_clock::now();

    for (int tid = 0; tid < args.thread_count; ++tid) {
        threads.emplace_back([tid, &args, &all_data, keys_per_thread, &thread_stats, barrier]() {
            RemoteH2DTest test(args.localip, args.port, tid, args.gpu_id);
            if (!test.Init()) {
                return;
            }
            test.SetDataParams(args.value_size);
            test.SetBarrier(barrier);

            int start_idx = tid * keys_per_thread;
            int end_idx = start_idx + keys_per_thread;
            test.SetSharedData(all_data, start_idx, end_idx);

            Stats stats;
            if (args.cmd == "rh2d") {
                test.RunRh2DBatch(args.batch, stats);
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
    std::cout << "Value size: " << args.value_size << " bytes" << std::endl;
    std::cout << "Total time: " << total_time_s << " s" << std::endl;
    std::cout << std::endl;

    double sum_avg_latency_us = 0;
    double sum_qps = 0;
    double sum_get_time_us = 0;
    double sum_h2d_time_us = 0;

    for (int tid = 0; tid < args.thread_count; ++tid) {
        const auto& s = thread_stats[tid];
        double avg_latency_us = s.batch_count > 0 ? s.total_time_us / s.batch_count : 0;
        double avg_latency_ms = avg_latency_us / 1000.0;
        double qps = s.total_time_us > 0 ? (s.key_count * 1000000.0 / s.total_time_us) : 0;

        sum_avg_latency_us += avg_latency_us;
        sum_qps += qps;
        if (args.cmd == "get") {
            sum_get_time_us += s.get_time_us;
            sum_h2d_time_us += s.h2d_time_us;
        }

        std::cout << "[T" << tid << "] Keys: " << s.key_count
                  << ", Batches: " << s.batch_count
                  << ", Avg Latency: " << std::fixed << std::setprecision(2) << avg_latency_ms << " ms"
                  << ", QPS: " << std::setprecision(1) << qps << " keys/s";
        if (args.cmd == "get") {
            double avg_get_us = s.batch_count > 0 ? s.get_time_us / s.batch_count : 0;
            double avg_h2d_us = s.batch_count > 0 ? s.h2d_time_us / s.batch_count : 0;
            std::cout << ", Avg Get: " << std::fixed << std::setprecision(2) << (avg_get_us/1000.0) << " ms"
                      << ", Avg H2D: " << std::setprecision(2) << (avg_h2d_us/1000.0) << " ms";
        }
        std::cout << std::endl;
    }

    std::cout << std::endl;
    double overall_avg_latency_us = sum_avg_latency_us / args.thread_count;
    double overall_qps = sum_qps;
    std::cout << "Overall Avg Latency: " << std::fixed << std::setprecision(2) << (overall_avg_latency_us/1000.0) << " ms" << std::endl;
    std::cout << "Overall QPS: " << std::setprecision(1) << overall_qps << " keys/s" << std::endl;
    if (args.cmd == "get") {
        double overall_get_time_us = sum_get_time_us / args.thread_count;
        double overall_h2d_time_us = sum_h2d_time_us / args.thread_count;
        std::cout << "Overall Avg Get Time: " << std::fixed << std::setprecision(2) << (overall_get_time_us/1000.0) << " ms" << std::endl;
        std::cout << "Overall Avg H2D Time: " << std::fixed << std::setprecision(2) << (overall_h2d_time_us/1000.0) << " ms" << std::endl;
    }
    std::cout << "=================================================" << std::endl;

    return 0;
}
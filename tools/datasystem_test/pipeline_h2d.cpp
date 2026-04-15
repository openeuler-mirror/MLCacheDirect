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

using namespace datasystem;

#define TLOG(tid, ...) do { \
    static std::mutex print_mutex; \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cout << "[T" << tid << "] "; \
    std::cout << __VA_ARGS__ << std::endl; \
} while(0)

#define TLOG_NONL(tid, ...) do { \
    static std::mutex print_mutex; \
    std::lock_guard<std::mutex> lock(print_mutex); \
    std::cout << "[T" << tid << "] "; \
    std::cout << __VA_ARGS__; \
} while(0)

#define TIMER_START(name) \
    auto timer_start_##name = std::chrono::high_resolution_clock::now();

#define TIMER_END(tid, name, desc) \
    { \
        auto timer_end_##name = std::chrono::high_resolution_clock::now(); \
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>( \
            timer_end_##name - timer_start_##name).count(); \
        TLOG(tid, desc << ": " << duration << " us"); \
        records.push_back(duration); \
    }

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

bool ParseBool(const std::string& str) {
    if (str == "true" || str == "1") return true;
    if (str == "false" || str == "0") return false;
    return true;
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

class H2DTest {
public:
    H2DTest(const std::string& host, int port = 18481, int thread_id = 0, int gpu_id = 0) 
        : thread_id_(thread_id), gpu_id_(gpu_id) {
        ConnectOptions connectOptions;
        connectOptions.host = host;
        connectOptions.port = port;
        connectOptions.accessKey = "";
        connectOptions.secretKey = "";
        connectOptions.deviceId = std::to_string(gpu_id);
        
        client_ = std::make_unique<KVClient>(connectOptions);
        client_->Init();
        TLOG(thread_id_, "Connected to " << host << ":" << port << " (GPU: " << gpu_id << ")");
    }

    void SetDataParams(const std::string& value_prefix, 
                      const std::vector<std::string>& custom_keys,
                      int count,
                      size_t value_size,
                      bool delete_value) {
        if (value_prefix.empty()) {
            base_value_prefix_ = "0";
        } else if (value_prefix.size() == 1) {
            base_value_prefix_ = value_prefix;
        } else {
            base_value_prefix_ = value_prefix.substr(0, 1);
            TLOG(thread_id_, "[WARN] value_prefix truncated to 1 char: '" << base_value_prefix_ << "'");
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

    void SetBarrier(std::shared_ptr<Barrier> barrier) { barrier_ = barrier; }

    void GenerateData(int count) {
        data_.clear();
        
        int actual_count = count;
        if (actual_count <= 0 && !custom_keys_.empty()) {
            actual_count = static_cast<int>(custom_keys_.size());
        }
        if (actual_count <= 0) actual_count = 10;
        
        std::string thread_value_prefix = base_value_prefix_ + "T" + std::to_string(thread_id_);
        
        for (int i = 0; i < actual_count; i++) {
            std::string base_key;
            if (i < static_cast<int>(custom_keys_.size()) && !custom_keys_[i].empty()) {
                base_key = custom_keys_[i];
            } else {
                base_key = std::to_string(i);
            }
            std::string key = base_key + "_T" + std::to_string(thread_id_);
            
            std::ostringstream oss;
            oss << "##########";
            int max_j = static_cast<int>(value_size_ / 10);
            for (int j = 1; j <= max_j; ++j) {
                std::string prefix = "|" + thread_value_prefix + "|";
                oss << prefix;
                size_t pad = 10 - prefix.size();
                oss << std::setfill('0') << std::setw(pad) << j;
            }
            std::string value = oss.str();
            if (value.size() > value_size_) {
                value = value.substr(0, value_size_);
            }
            data_.emplace_back(key, value);
        }
        TLOG(thread_id_, "generate data ok, count=" << actual_count 
                  << ", value_prefix='" << thread_value_prefix << "'"
                  << ", value_size=" << value_size_);
    }

    void RunSet(int count = 10) {
        GenerateData(count);
        
        int failed = 0;
        for (const auto& it : data_) {
            Status rc = client_->Set(it.first, it.second);
            if (rc.IsError()) {
                failed++;
                TLOG(thread_id_, rc.GetMsg());
            }
        }
        TLOG_NONL(thread_id_, "Set " << count << " keys completed.");
        if (failed) TLOG_NONL(thread_id_, " failed " << failed);
        
        TLOG(thread_id_, "");
        TLOG_NONL(thread_id_, "Set keys: ");
        for (size_t i = 0; i < data_.size(); ++i) {
            if (i > 0) std::cout << ", ";
            std::cout << data_[i].first;
            if (i >= 9 && data_.size() > 10) {
                std::cout << ", ... (" << (data_.size() - 10) << " more)";
                break;
            }
        }
        std::cout << std::endl;
    }

    void RunMGetH2D(int count, int timeout_ms = 6000000) {
        GenerateData(count);
        
        if (barrier_) barrier_->Wait();
        
        std::vector<std::string> keys;
        std::vector<std::pair<void*, size_t>> devShmChunks;
        std::vector<std::string> outFailedKeys;
        cudaError_t err;

        int gpu_id = gpu_id_;
        if ((err = cudaSetDevice(gpu_id)) != cudaSuccess) {
            TLOG(thread_id_, "cudaSetDevice(" << gpu_id << ") failed: " << cudaGetErrorString(err));
            return;
        }
        TLOG(thread_id_, "Using GPU " << gpu_id);

        void* dev_ptr = nullptr;
        for (const auto& it : data_) {
            keys.push_back(it.first);
            if ((err = cudaMalloc(&dev_ptr, it.second.size())) != cudaSuccess) {
                TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                return;
            }
            TLOG(thread_id_, "Allocated device memory at: " << dev_ptr);
            devShmChunks.push_back({dev_ptr, it.second.size()});
        }
        
        TIMER_START(Mget);
        auto ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys, timeout_ms);
        TIMER_END(thread_id_, Mget, "MGETH2D");

        if (ret == datasystem::Status::OK()) {
            TLOG(thread_id_, "MGetH2D success!"); 
        } else {
            TLOG(thread_id_, "MGetH2D completed. Failed keys: " << outFailedKeys.size());
            TLOG(thread_id_, "last error is " << ret.GetMsg());
        }

        if (delete_value_) {
            std::vector<std::string> failkeys;
            Status rc = client_->Del(keys, failkeys);
            if (rc.IsError()) {
                TLOG(thread_id_, "del failed " << rc.GetMsg());
                return;
            }
        }

        if (outFailedKeys.empty()) {
            CheckData(count, devShmChunks);
        }

        for (auto& chunk : devShmChunks) {
            cudaFree(chunk.first);
        }
    }

    void MGetH2DBatch(int count, int batch, int timeout_ms = 6000000) {
        GenerateData(count);
        
        if (barrier_) barrier_->Wait();
        
        std::vector<std::string> keys;
        std::vector<std::pair<void*, size_t>> devShmChunks;
        std::vector<std::string> outFailedKeys;
        cudaError_t err;

        int gpu_id = gpu_id_;
        if ((err = cudaSetDevice(gpu_id)) != cudaSuccess) {
            TLOG(thread_id_, "cudaSetDevice(" << gpu_id << ") failed: " << cudaGetErrorString(err));
            return;
        }

        for (int round = 0; round < count / batch; round++) {
            keys.clear();
            outFailedKeys.clear();
            devShmChunks.clear();
            
            for (int i = round * batch; i < (round + 1) * batch; i++) {
                const auto& it = data_[i];
                keys.push_back(it.first);
                
                void* dev_ptr = nullptr;
                if ((err = cudaMalloc(&dev_ptr, it.second.size())) != cudaSuccess) {
                    TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                    return;
                }
                TLOG(thread_id_, "Allocated device memory at: " << dev_ptr);
                devShmChunks.push_back({dev_ptr, it.second.size()});
            }

            TIMER_START(round);
            auto ret = client_->MGetH2D(keys, devShmChunks, outFailedKeys, timeout_ms);
            TIMER_END(thread_id_, round, "MGETH2D");
            
            if (ret == datasystem::Status::OK()) {
                TLOG(thread_id_, "MGetH2D success!"); 
            } else {
                TLOG(thread_id_, "MGetH2D completed. Failed keys: " << outFailedKeys.size());
                TLOG(thread_id_, "last error is " << ret.GetMsg());
            }

            if (outFailedKeys.empty()) {
                CheckDataBatch(round * batch, devShmChunks);
            }

            for (auto& chunk : devShmChunks) {
                cudaFree(chunk.first);
            }
        }
    }

    void Get(int count, int timeout_ms = 600000) {
        GenerateData(count);
        
        if (barrier_) barrier_->Wait();
        
        std::vector<std::string> keys;
        std::vector<std::string> failkeys;
        std::vector<std::string> values;
        cudaError_t err;
        std::vector<void*> dev_ptrs;

        int gpu_id = gpu_id_;
        if ((err = cudaSetDevice(gpu_id)) != cudaSuccess) {
            TLOG(thread_id_, "cudaSetDevice(" << gpu_id << ") failed: " << cudaGetErrorString(err));
            return;
        }

        for (const auto& it : data_) {
            keys.push_back(it.first);
            void* dev_ptr = nullptr;
            if ((err = cudaMalloc(&dev_ptr, it.second.size() + 1)) != cudaSuccess) {
                TLOG(thread_id_, "cudaMalloc failed: " << cudaGetErrorString(err));
                return;
            }
            TLOG(thread_id_, "Allocated device memory at: " << dev_ptr);
            dev_ptrs.push_back(dev_ptr);
        }

        TIMER_START(Get);
        Status rc = client_->Get(keys, values, timeout_ms);
        TIMER_END(thread_id_, Get, "Get");
        
        TIMER_START(H2D);
        for (int i = 0; i < count; i++) {
            if ((err = cudaMemcpy(dev_ptrs[i], values[i].c_str(), 
                    (values[i].length() + 1) * sizeof(char), cudaMemcpyHostToDevice)) != cudaSuccess) {
                TLOG(thread_id_, "failed to do cudaMemcpy for " << i << "th: " << cudaGetErrorString(err));
            }
        }
        TIMER_END(thread_id_, H2D, "H2D");
        
        if (rc.IsError()) {
            TLOG(thread_id_, "get failed " << rc.GetMsg());
            return;
        }
        
        if (delete_value_) {
            rc = client_->Del(keys, failkeys);
            if (rc.IsError()) {
                TLOG(thread_id_, "del failed " << rc.GetMsg());
                return;
            }
        }
        
        for (int i = 0; i < count; i++) {
            if (values[i] != data_[i].second) {
                TLOG(thread_id_, "############################## " << i << " th data is not same ###############################");
                std::ofstream a("/tmp/expect_T" + std::to_string(thread_id_)), 
                              b("/tmp/real_T" + std::to_string(thread_id_));
                auto& data = data_[i].second;
                for (size_t j = 0; j < data.size(); j += 80) {
                    size_t len = (j + 80 < data.size()) ? 80 : (data.size() - j);
                    a << data.substr(j, len) << std::endl;
                    b << values[i].substr(j, len) << std::endl;
                }
                a.close(); b.close();
                std::system(("diff /tmp/expect_T" + std::to_string(thread_id_) + 
                           " /tmp/real_T" + std::to_string(thread_id_)).c_str());
            }
        }
    }

    int GetThreadId() const { return thread_id_; }

private:
    void CheckData(int count, const std::vector<std::pair<void*, size_t>>& devShmChunks) {
        bool is_failed = false;
        for (int i = 0; i < count; i++) {
            void* ptr = devShmChunks[i].first;
            size_t size = devShmChunks[i].second;
            auto& data = data_[i].second;
            std::string readOutData;
            cudaError_t err;

            readOutData.resize(size);
            if ((err = cudaMemcpy(reinterpret_cast<void*>(readOutData.data()), ptr, size, cudaMemcpyDeviceToHost)) != cudaSuccess) {
                TLOG(thread_id_, "failed to do cudaMemcpy for " << i << "th: " << cudaGetErrorString(err));
                is_failed = true;
            }
            if (readOutData != data) {
                TLOG(thread_id_, "############################## " << i << " th data is not same ###############################");
                std::ofstream a("/tmp/expect_T" + std::to_string(thread_id_)), 
                              b("/tmp/real_T" + std::to_string(thread_id_));
                for (size_t j = 0; j < data.size(); j += 80) {
                    size_t len = (j + 80 < data.size()) ? 80 : (data.size() - j);
                    a << data.substr(j, len) << std::endl;
                    b << readOutData.substr(j, len) << std::endl;
                }
                a.close(); b.close();
                std::system(("diff /tmp/expect_T" + std::to_string(thread_id_) + 
                           " /tmp/real_T" + std::to_string(thread_id_)).c_str());
                is_failed = true;
            }
        }
        if (is_failed) {
            TLOG(thread_id_, "CheckData failed!");
        } else {
            TLOG(thread_id_, "CheckData success!");
        }
    }

    void CheckDataBatch(int startIdx, const std::vector<std::pair<void*, size_t>>& devShmChunks) {
        bool is_failed = false;
        for (size_t i = 0; i < devShmChunks.size(); i++) {
            void* ptr = devShmChunks[i].first;
            size_t size = devShmChunks[i].second;
            auto& data = data_[startIdx + i].second;
            std::string readOutData;
            cudaError_t err;

            readOutData.resize(size);
            if ((err = cudaMemcpy(reinterpret_cast<void*>(readOutData.data()), ptr, size, cudaMemcpyDeviceToHost)) != cudaSuccess) {
                TLOG(thread_id_, "failed to do cudaMemcpy for " << (startIdx + i) << "th: " << cudaGetErrorString(err));
                is_failed = true;
            }
            if (readOutData != data) {
                TLOG(thread_id_, "############################## " << (startIdx + i) << " th data is not same ###############################");
                is_failed = true;
            }
        }
        if (is_failed) {
            TLOG(thread_id_, "CheckDataBatch failed!");
        } else {
            TLOG(thread_id_, "CheckDataBatch success!");
        }
    }

    std::unique_ptr<KVClient> client_;
    std::vector<std::pair<void*, size_t>> devShmChunks_;
    std::vector<std::pair<std::string, std::string>> data_;
    std::vector<long long> records;
    
    std::string base_value_prefix_ = "0";
    std::vector<std::string> custom_keys_;
    int count_ = 10;
    size_t value_size_ = 8388608;
    int thread_id_ = 0;
    int gpu_id_ = 0;
    bool delete_value_ = true;
    std::shared_ptr<Barrier> barrier_;
};

void RunWorker(const std::string& host, int port,
               const std::string& cmd, 
               int count, int batch, const std::string& value_prefix,
               const std::vector<std::string>& keys, size_t value_size,
               bool delete_value, int thread_id, 
               std::shared_ptr<Barrier> barrier,
               int gpu_id) {
    try {
        H2DTest bench(host, port, thread_id, gpu_id);
        bench.SetDataParams(value_prefix, keys, count, value_size, delete_value);
        bench.SetBarrier(barrier);
        
        if (cmd == "set") {
            bench.RunSet(count);
        } else if (cmd == "get" || cmd == "mgeth2d") {
            bench.RunMGetH2D(count);
        } else if (cmd == "batchget") {
            bench.MGetH2DBatch(count, batch);
        } else if (cmd == "originget") {
            bench.Get(count);
        } else {
            TLOG(thread_id, "Unknown command: " << cmd);
        }
    } catch (const std::exception& e) {
        TLOG(thread_id, "Error: " << e.what());
    }
}

void PrintUsage(const char* prog) {
    std::cout << "Usage: " << prog << " <host> [options] [command]" << std::endl;
    std::cout << "  host    : Server IP address (required, positional)" << std::endl;
    std::cout << "  command : set | get | mgeth2d | batchget | originget" << std::endl;
    std::cout << "Options:" << std::endl;
    std::cout << "  --port=N or --port N            : Server port (default: 18481)" << std::endl;
    std::cout << "  --count=N or --count N          : Number of keys per thread (default: 10)" << std::endl;
    std::cout << "  --batch=N or --batch N          : Batch size for batchget (default: 10)" << std::endl;
    std::cout << "  --value_prefix=X or --value_prefix X  : Base prefix for value (default: 0)" << std::endl;
    std::cout << "  --keys=k1,k2... or --keys k1,k2     : Comma-separated custom key list" << std::endl;
    std::cout << "  --value_size=N or --value_size N    : Length of generated value (default: 8388608)" << std::endl;
    std::cout << "  --thread=N or --thread N            : Number of concurrent threads (default: 1)" << std::endl;
    std::cout << "  --delete_value=true|false|1|0       : Delete keys after get (default: true)" << std::endl;
    std::cout << "  --gpu_id=N                         : GPU device ID to use (default: 0)" << std::endl;
    std::cout << "  --help or -h                        : Show this help message (can be placed anywhere)" << std::endl;
    std::cout << "Examples:" << std::endl;
    std::cout << "  " << prog << " 141.61.91.188 --port=18581 set --keys 123,456 --count 4 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4" << std::endl;
    std::cout << "  " << prog << " 141.61.91.189 --port=18581 get --keys 123,456 --count 4 --value_prefix a --value_size 8388608 --gpu_id 0 --thread 4 --delete_value false" << std::endl;
    std::cout << "  " << prog << " 141.61.91.188 --port=18581 set --keys 123,456 --count 4 --value_prefix b --value_size 8388608 --gpu_id 0 --thread 4" << std::endl;
    std::cout << "  " << prog << " 141.61.91.189 --port=18581 originget --keys 123,456 --count 4 --value_prefix b --value_size 8388608 --gpu_id 0 --thread 4 --delete_value false" << std::endl;
}

struct CmdArgs {
    std::string host;
    int port = 18481;
    std::string cmd = "full";
    int count = 10;
    int batch = 10;
    std::string value_prefix = "0";
    std::vector<std::string> keys;
    size_t value_size = 8388608;
    int thread_count = 1;
    bool delete_value = true;
    std::string gpu_id_str = "0";
    int gpu_id = 0;
    bool help = false;
};

CmdArgs ParseArgs(int argc, char* argv[]) {
    CmdArgs args;
    if (argc < 2) return args;
    
    args.host = argv[1];
    
    for (int i = 2; i < argc; i++) {
        std::string arg = argv[i];
        std::string key, value;
        
        // 【修改点1】优先检查 --help，支持在任何位置触发
        if (arg == "--help" || arg == "-h") {
            args.help = true;
            return args;
        }
        
        if (ParseKeyValue(arg, key, value)) {
            if (key == "count") args.count = std::stoi(value);
            else if (key == "batch") args.batch = std::stoi(value);
            else if (key == "value_prefix") args.value_prefix = value;
            else if (key == "keys") args.keys = SplitString(value, ',');
            else if (key == "value_size") args.value_size = std::stoull(value);
            else if (key == "thread") args.thread_count = std::stoi(value);
            else if (key == "delete_value") args.delete_value = ParseBool(value);
            else if (key == "gpu_id" || key == "gpu_num") args.gpu_id_str = value;
            else if (key == "port") args.port = std::stoi(value);
            continue;
        }
        
        // 解析 --key value 格式
        if (arg.size() >= 3 && arg[0] == '-' && arg[1] == '-') {
            key = arg.substr(2);
            
            // 【修改点1】先检查下一个参数是否为 --help，避免被误解析为 value
            if (i + 1 < argc) {
                std::string next_arg = argv[i + 1];
                if (next_arg == "--help" || next_arg == "-h") {
                    args.help = true;
                    return args;
                }
            }
            
            if (i + 1 < argc && argv[i + 1][0] != '-') {
                value = argv[++i];
                if (key == "count") args.count = std::stoi(value);
                else if (key == "batch") args.batch = std::stoi(value);
                else if (key == "value_prefix") args.value_prefix = value;
                else if (key == "keys") args.keys = SplitString(value, ',');
                else if (key == "value_size") args.value_size = std::stoull(value);
                else if (key == "thread") args.thread_count = std::stoi(value);
                else if (key == "delete_value") args.delete_value = ParseBool(value);
                else if (key == "gpu_id") args.gpu_id_str = value;
                else if (key == "port") args.port = std::stoi(value);
            }
        } else if (arg[0] != '-' && args.cmd == "full") {
            args.cmd = arg;
        }
    }
    
    return args;
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        PrintUsage(argv[0]);
        return 1;
    }

    CmdArgs args = ParseArgs(argc, argv);
    
    if (args.help) {
        PrintUsage(argv[0]);
        return 0;
    }
    
    // 解析 GPU 索引，默认为 0
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
    
    // 端口合法性检查
    if (args.port <= 0 || args.port > 65535) {
        std::cerr << "[ERROR] Invalid port: " << args.port << ", must be 1-65535" << std::endl;
        return 1;
    }
    
    if (args.cmd == "batchget" && args.count % args.batch != 0) {
        std::cout << "ERROR: count % batch != 0" << std::endl;
        return 1;
    }
    
    if (args.thread_count < 1) {
        std::cout << "ERROR: thread count must be >= 1" << std::endl;
        return 1;
    }

    std::cout << "[Main] Starting with " << args.thread_count << " thread(s), " 
              << "target GPU: " << args.gpu_id << ", "
              << "port: " << args.port << ", "
              << "command=" << args.cmd << std::endl;

    try {
        auto barrier = std::make_shared<Barrier>(args.thread_count);
        std::vector<std::thread> threads;
        
        for (int tid = 0; tid < args.thread_count; ++tid) {
            threads.emplace_back(RunWorker, 
                               args.host, args.port,
                               args.cmd,
                               args.count, args.batch, 
                               args.value_prefix, args.keys, args.value_size,
                               args.delete_value, tid, barrier, args.gpu_id);
        }
        
        for (auto& t : threads) {
            if (t.joinable()) t.join();
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    std::cout << "[Main] All threads completed." << std::endl;
    return 0;
}
#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"

#include <cuda_runtime.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

using datasystem::ConnectOptions;
using datasystem::KVClient;
using datasystem::Optional;
using datasystem::ReadOnlyBuffer;
using datasystem::SetParam;
using datasystem::Status;

namespace {

std::mutex g_logMutex;

struct Options {
    std::string host;
    std::string command;
    std::string keyPrefix = "async_pin";
    int port = 18481;
    int count = 1;
    uint64_t valueSize = 3670016;
    int gpuId = 0;
    int threadNum = 1;
    int timeoutMs = 60000;
    bool enableLocalCache = true;
    bool cleanupBefore = true;
    bool deleteAfter = false;
    bool help = false;
};

struct Summary {
    std::atomic<int> createOk{ 0 };
    std::atomic<int> setOk{ 0 };
    std::atomic<int> getOk{ 0 };
    std::atomic<int> h2dOk{ 0 };
    std::atomic<int> d2hOk{ 0 };
    std::atomic<int> verifyOk{ 0 };
    std::atomic<int> failed{ 0 };
};

class DeviceBuffer {
public:
    ~DeviceBuffer()
    {
        if (data_ != nullptr) {
            (void)cudaFree(data_);
        }
    }

    cudaError_t Allocate(size_t size)
    {
        size_ = size;
        return cudaMalloc(&data_, size_);
    }

    void *Data() const
    {
        return data_;
    }

private:
    void *data_ = nullptr;
    size_t size_ = 0;
};

template <typename... Args>
void Log(int tid, Args &&...args)
{
    std::ostringstream stream;
    (stream << ... << args);
    std::lock_guard<std::mutex> lock(g_logMutex);
    std::cout << "[T" << tid << "] " << stream.str() << std::endl;
}

template <typename Func>
int64_t MeasureUs(Func &&func)
{
    const auto begin = std::chrono::steady_clock::now();
    func();
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now() - begin)
        .count();
}

bool ParseBool(const std::string &value, bool &result)
{
    if (value == "true" || value == "1") {
        result = true;
        return true;
    }
    if (value == "false" || value == "0") {
        result = false;
        return true;
    }
    return false;
}

bool ReadOptionValue(int argc, char **argv, int &index, std::string &name, std::string &value)
{
    std::string argument = argv[index];
    if (argument.rfind("--", 0) != 0) {
        return false;
    }
    const size_t equal = argument.find('=');
    name = argument.substr(2, equal == std::string::npos ? std::string::npos : equal - 2);
    if (equal != std::string::npos) {
        value = argument.substr(equal + 1);
        return true;
    }
    if (index + 1 >= argc) {
        return false;
    }
    value = argv[++index];
    return true;
}

bool ApplyBoolOption(const std::string &name, const std::string &value, Options &options)
{
    if (name == "enable_local_cache") {
        return ParseBool(value, options.enableLocalCache);
    }
    if (name == "cleanup_before") {
        return ParseBool(value, options.cleanupBefore);
    }
    if (name == "delete_after") {
        return ParseBool(value, options.deleteAfter);
    }
    return false;
}

bool ApplyOption(const std::string &name, const std::string &value, Options &options)
{
    if (name == "port") {
        options.port = std::stoi(value);
    } else if (name == "count") {
        options.count = std::stoi(value);
    } else if (name == "value_size") {
        options.valueSize = std::stoull(value);
    } else if (name == "gpu_id") {
        options.gpuId = std::stoi(value);
    } else if (name == "thread") {
        options.threadNum = std::stoi(value);
    } else if (name == "timeout_ms") {
        options.timeoutMs = std::stoi(value);
    } else if (name == "key_prefix") {
        options.keyPrefix = value;
    } else {
        return ApplyBoolOption(name, value, options);
    }
    return true;
}

bool ParseArgs(int argc, char **argv, Options &options)
{
    if (argc < 3) {
        return false;
    }
    options.host = argv[1];
    options.command = argv[2];
    for (int i = 3; i < argc; ++i) {
        const std::string argument = argv[i];
        if (argument == "--help" || argument == "-h") {
            options.help = true;
            return true;
        }
        std::string name;
        std::string value;
        if (!ReadOptionValue(argc, argv, i, name, value) || !ApplyOption(name, value, options)) {
            std::cerr << "Invalid option: " << argument << std::endl;
            return false;
        }
    }
    return true;
}

bool ValidateOptions(const Options &options)
{
    const bool validCommand = options.command == "set" || options.command == "get"
                              || options.command == "roundtrip";
    if (!validCommand || options.port <= 0 || options.port > 65535) {
        return false;
    }
    return options.count > 0 && options.valueSize > 0 && options.threadNum > 0 && options.gpuId >= 0
           && options.timeoutMs > 0;
}

void PrintUsage(const char *program)
{
    std::cout << "Usage: " << program << " <host> <set|get|roundtrip> [options]\n"
              << "  --port=N                 Worker port, default 18481\n"
              << "  --count=N                Keys per thread, default 1\n"
              << "  --value_size=N           Bytes per value, default 3670016\n"
              << "  --key_prefix=TEXT        Key prefix, default async_pin\n"
              << "  --gpu_id=N               CUDA device, default 0\n"
              << "  --thread=N               Concurrent threads, default 1\n"
              << "  --timeout_ms=N           Get timeout, default 60000\n"
              << "  --enable_local_cache=B   Local-cache mode, default true\n"
              << "  --cleanup_before=B       Delete keys before Set, default true\n"
              << "  --delete_after=B         Delete keys after test, default false\n";
}

uint32_t HashKey(const std::string &key)
{
    uint32_t hash = 2166136261U;
    for (unsigned char value : key) {
        hash = (hash ^ value) * 16777619U;
    }
    return hash;
}

std::vector<uint8_t> MakeExpectedData(const std::string &key, size_t size)
{
    std::vector<uint8_t> data(size);
    uint32_t state = HashKey(key);
    for (size_t i = 0; i < size; ++i) {
        state = state * 1664525U + 1013904223U;
        data[i] = static_cast<uint8_t>((state >> 24U) ^ static_cast<uint32_t>(i));
    }
    return data;
}

std::string BuildKey(const Options &options, int tid, int index)
{
    return options.keyPrefix + "_T" + std::to_string(tid) + "_" + std::to_string(index);
}

bool CheckCuda(cudaError_t error, int tid, const std::string &operation)
{
    if (error == cudaSuccess) {
        return true;
    }
    Log(tid, "[CUDA_ERROR] operation=", operation, " detail=", cudaGetErrorString(error));
    return false;
}

void DeleteKey(KVClient &client, const std::string &key, int tid)
{
    std::vector<std::string> failedKeys;
    Status rc = client.Del({ key }, failedKeys);
    if (rc.IsError() && !failedKeys.empty()) {
        Log(tid, "[DELETE_WARN] key=", key, " rc=", rc.ToString());
    }
}

bool CopyExpectedToDevice(const std::vector<uint8_t> &expected, DeviceBuffer &device, int tid, Summary &summary)
{
    cudaError_t error = cudaSuccess;
    const int64_t elapsed = MeasureUs([&] {
        error = cudaMemcpy(device.Data(), expected.data(), expected.size(), cudaMemcpyHostToDevice);
    });
    if (!CheckCuda(error, tid, "expected H2D")) {
        return false;
    }
    ++summary.h2dOk;
    Log(tid, "[H2D] phase=set_input size=", expected.size(), " elapsed_us=", elapsed);
    return true;
}

bool CopyDeviceToCreateBuffer(DeviceBuffer &device, const std::shared_ptr<datasystem::Buffer> &buffer, int tid,
                              Summary &summary)
{
    cudaError_t error = cudaSuccess;
    const int64_t elapsed = MeasureUs([&] {
        error = cudaMemcpy(buffer->MutableData(), device.Data(), buffer->GetSize(), cudaMemcpyDeviceToHost);
    });
    if (!CheckCuda(error, tid, "Create buffer D2H")) {
        return false;
    }
    ++summary.d2hOk;
    Log(tid, "[D2H] phase=create_buffer size=", buffer->GetSize(), " elapsed_us=", elapsed);
    return true;
}

bool RunSetOne(KVClient &client, const Options &options, const std::string &key, int tid, Summary &summary)
{
    if (options.cleanupBefore) {
        DeleteKey(client, key, tid);
    }
    const auto expected = MakeExpectedData(key, options.valueSize);
    DeviceBuffer device;
    if (!CheckCuda(device.Allocate(expected.size()), tid, "cudaMalloc for Set")) {
        return false;
    }
    if (!CopyExpectedToDevice(expected, device, tid, summary)) {
        return false;
    }
    std::shared_ptr<datasystem::Buffer> buffer;
    Status rc;
    const int64_t createUs = MeasureUs([&] { rc = client.Create(key, expected.size(), SetParam{}, buffer); });
    Log(tid, "[CREATE] key=", key, " rc=", rc.ToString(), " elapsed_us=", createUs);
    if (rc.IsError() || buffer == nullptr) {
        return false;
    }
    ++summary.createOk;
    if (!CopyDeviceToCreateBuffer(device, buffer, tid, summary)) {
        return false;
    }
    const int64_t setUs = MeasureUs([&] { rc = client.Set(buffer); });
    Log(tid, "[SET] key=", key, " rc=", rc.ToString(), " elapsed_us=", setUs);
    if (rc.IsError()) {
        return false;
    }
    ++summary.setOk;
    return true;
}

bool CopyReadBufferThroughGpu(ReadOnlyBuffer &buffer, std::vector<uint8_t> &actual, int tid, Summary &summary)
{
    DeviceBuffer device;
    if (!CheckCuda(device.Allocate(buffer.GetSize()), tid, "cudaMalloc for Get")) {
        return false;
    }
    cudaError_t error = cudaSuccess;
    const int64_t h2dUs = MeasureUs([&] {
        error = cudaMemcpy(device.Data(), buffer.ImmutableData(), buffer.GetSize(), cudaMemcpyHostToDevice);
    });
    if (!CheckCuda(error, tid, "ReadOnlyBuffer H2D")) {
        return false;
    }
    ++summary.h2dOk;
    const int64_t d2hUs = MeasureUs([&] {
        error = cudaMemcpy(actual.data(), device.Data(), actual.size(), cudaMemcpyDeviceToHost);
    });
    if (!CheckCuda(error, tid, "verification D2H")) {
        return false;
    }
    ++summary.d2hOk;
    Log(tid, "[CUDA_COPY] phase=get h2d_us=", h2dUs, " verify_d2h_us=", d2hUs);
    return true;
}

bool RunGetOne(KVClient &client, const Options &options, const std::string &key, int tid, Summary &summary)
{
    Optional<ReadOnlyBuffer> buffer;
    Status rc;
    const int64_t getUs = MeasureUs([&] { rc = client.Get(key, buffer, options.timeoutMs); });
    Log(tid, "[GET] key=", key, " rc=", rc.ToString(), " elapsed_us=", getUs);
    if (rc.IsError() || !buffer) {
        return false;
    }
    ++summary.getOk;
    const auto expected = MakeExpectedData(key, options.valueSize);
    if (buffer->GetSize() != static_cast<int64_t>(expected.size())) {
        Log(tid, "[VERIFY_FAIL] key=", key, " expected_size=", expected.size(), " actual_size=", buffer->GetSize());
        return false;
    }
    std::vector<uint8_t> actual(expected.size());
    if (!CopyReadBufferThroughGpu(*buffer, actual, tid, summary)) {
        return false;
    }
    if (actual != expected) {
        Log(tid, "[VERIFY_FAIL] key=", key, " data mismatch");
        return false;
    }
    ++summary.verifyOk;
    Log(tid, "[VERIFY] key=", key, " PASS");
    return true;
}

bool RunOne(KVClient &client, const Options &options, const std::string &key, int tid, Summary &summary)
{
    bool success = true;
    if (options.command == "set" || options.command == "roundtrip") {
        success = RunSetOne(client, options, key, tid, summary);
    }
    if (success && (options.command == "get" || options.command == "roundtrip")) {
        success = RunGetOne(client, options, key, tid, summary);
    }
    if (options.deleteAfter) {
        DeleteKey(client, key, tid);
    }
    if (!success) {
        ++summary.failed;
        Log(tid, "[RESULT] key=", key, " FAIL");
    }
    return success;
}

void RunThread(const std::shared_ptr<KVClient> &client, const Options &options, int tid, Summary &summary)
{
    if (!CheckCuda(cudaSetDevice(options.gpuId), tid, "cudaSetDevice")) {
        summary.failed += options.count;
        return;
    }
    for (int index = 0; index < options.count; ++index) {
        const std::string key = BuildKey(options, tid, index);
        (void)RunOne(*client, options, key, tid, summary);
    }
}

std::shared_ptr<KVClient> InitClient(const Options &options, int64_t &elapsedUs)
{
    ConnectOptions connect;
    connect.host = options.host;
    connect.port = options.port;
    connect.deviceId = std::to_string(options.gpuId);
    connect.enableLocalCache = options.enableLocalCache;
    auto client = std::make_shared<KVClient>(connect);
    Status rc;
    elapsedUs = MeasureUs([&] { rc = client->Init(); });
    if (rc.IsError()) {
        std::cerr << "[INIT_FAIL] " << rc.ToString() << std::endl;
        return nullptr;
    }
    return client;
}

void PrintSummary(const Options &options, const Summary &summary, int64_t initUs)
{
    std::lock_guard<std::mutex> lock(g_logMutex);
    std::cout << "========== TEST SUMMARY ==========\n"
              << "command           : " << options.command << '\n'
              << "threads           : " << options.threadNum << '\n'
              << "keys per thread   : " << options.count << '\n'
              << "value size        : " << options.valueSize << '\n'
              << "enable local cache: " << std::boolalpha << options.enableLocalCache << '\n'
              << "init us           : " << initUs << '\n'
              << "create success    : " << summary.createOk.load() << '\n'
              << "set success       : " << summary.setOk.load() << '\n'
              << "get success       : " << summary.getOk.load() << '\n'
              << "H2D success       : " << summary.h2dOk.load() << '\n'
              << "D2H success       : " << summary.d2hOk.load() << '\n'
              << "verify success    : " << summary.verifyOk.load() << '\n'
              << "failed            : " << summary.failed.load() << '\n'
              << "result            : " << (summary.failed.load() == 0 ? "PASS" : "FAIL") << std::endl;
}

int Run(const Options &options)
{
    int64_t initUs = 0;
    auto client = InitClient(options, initUs);
    if (client == nullptr) {
        return 1;
    }
    std::cout << "[INIT] rc=OK elapsed_us=" << initUs << std::endl;
    Summary summary;
    std::vector<std::thread> threads;
    for (int tid = 0; tid < options.threadNum; ++tid) {
        threads.emplace_back(RunThread, client, std::cref(options), tid, std::ref(summary));
    }
    for (auto &thread : threads) {
        thread.join();
    }
    PrintSummary(options, summary, initUs);
    return summary.failed.load() == 0 ? 0 : 2;
}

}  // namespace

int main(int argc, char **argv)
{
    Options options;
    try {
        if (!ParseArgs(argc, argv, options) || options.help || !ValidateOptions(options)) {
            PrintUsage(argv[0]);
            return options.help ? 0 : 1;
        }
    } catch (const std::exception &error) {
        std::cerr << "Invalid argument: " << error.what() << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }
    return Run(options);
}

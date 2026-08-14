#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/service_discovery.h"

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
#include <unordered_map>
#include <vector>

using datasystem::ConnectOptions;
using datasystem::DataPlacementPolicy;
using datasystem::KVClient;
using datasystem::Optional;
using datasystem::ReadOnlyBuffer;
using datasystem::ServiceAffinityPolicy;
using datasystem::ServiceDiscovery;
using datasystem::ServiceDiscoveryOptions;
using datasystem::SetParam;
using datasystem::Status;

namespace {

std::mutex g_logMutex;

struct Options {
    std::string host;
    std::string command;
    std::string etcdAddress;
    std::string clusterName;
    std::string hostIdEnvName;
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
    } else if (name == "etcd_address") {
        options.etcdAddress = value;
    } else if (name == "cluster_name") {
        options.clusterName = value;
    } else if (name == "host_id_env_name") {
        options.hostIdEnvName = value;
    } else {
        return ApplyBoolOption(name, value, options);
    }
    return true;
}

bool ParseArgs(int argc, char **argv, Options &options)
{
    if (argc < 2) {
        return false;
    }
    int optionBegin = 0;
    const std::string first = argv[1];
    if (first == "set" || first == "get" || first == "roundtrip" || first == "shell") {
        options.command = first;
        optionBegin = 2;
    } else if (argc >= 3) {
        options.host = first;
        options.command = argv[2];
        optionBegin = 3;
    } else {
        return false;
    }
    for (int i = optionBegin; i < argc; ++i) {
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
                              || options.command == "roundtrip" || options.command == "shell";
    const bool hasServiceDiscovery = !options.etcdAddress.empty() || !options.clusterName.empty();
    const bool validConnection = hasServiceDiscovery
                                     ? !options.etcdAddress.empty() && !options.clusterName.empty()
                                     : !options.host.empty() && options.port > 0 && options.port <= 65535;
    if (!validCommand || !validConnection) {
        return false;
    }
    return options.count > 0 && options.valueSize > 0 && options.threadNum > 0 && options.gpuId >= 0
           && options.timeoutMs > 0;
}

void PrintUsage(const char *program)
{
    std::cout << "Usage:\n"
              << "  " << program << " <host> <set|get|roundtrip|shell> [options]\n"
              << "  " << program << " <set|get|roundtrip|shell> --etcd_address=ADDR --cluster_name=NAME [options]\n"
              << "  --port=N                 Worker port, default 18481\n"
              << "  --etcd_address=ADDR      ETCD address list for service discovery\n"
              << "  --cluster_name=NAME      Datasystem cluster name for service discovery\n"
              << "  --host_id_env_name=NAME  Environment variable containing the local host ID\n"
              << "  --count=N                Keys per thread, default 1\n"
              << "  --value_size=N           Bytes per value, default 3670016\n"
              << "  --key_prefix=TEXT        Key prefix, default async_pin\n"
              << "  --gpu_id=N               CUDA device, default 0\n"
              << "  --thread=N               Concurrent threads, default 1\n"
              << "  --timeout_ms=N           Get timeout, default 60000\n"
              << "  --enable_local_cache=B   Local-cache mode, default true\n"
              << "  --cleanup_before=B       Delete keys before Set, default true\n"
              << "  --delete_after=B         Delete keys after test, default false\n"
              << "\nShell commands:\n"
              << "  create <key> [size]       Create and fill a Buffer, but do not Set it\n"
              << "  set <key>                 Set a Buffer created by the create command\n"
              << "  get <key> [size]          Get, H2D, D2H and verify a value\n"
              << "  roundtrip <key> [size]    Create, Set, Get and verify a value\n"
              << "  mcreate <prefix> <count> [size]  Batch Create and fill Buffers\n"
              << "  mset <prefix>             Batch Set Buffers from mcreate\n"
              << "  mget <prefix> <count> [size]  Batch Get and verify values\n"
              << "  mroundtrip <prefix> <count> [size]  MCreate, MSet and MGet\n"
              << "  parallel <op> <prefix> <count> <threads> [size]\n"
              << "  del <key>                 Delete a published value\n"
              << "  discard <key>             Release a pending Create Buffer without Set\n"
              << "  pending                   List Buffers waiting for Set\n"
              << "  sleep <ms>                Keep the client alive and wait\n"
              << "  status                    Print cumulative counters\n"
              << "  help                      Print shell commands\n"
              << "  quit                      Destroy the client and exit\n";
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

bool CopyExpectedToCreateBuffer(const std::vector<uint8_t> &expected,
                                const std::shared_ptr<datasystem::Buffer> &buffer, int tid)
{
    void *destination = buffer == nullptr ? nullptr : buffer->MutableData();
    if (destination == nullptr || buffer->GetSize() != static_cast<int64_t>(expected.size())) {
        Log(tid, "[HOST_COPY_FAIL] invalid Create Buffer");
        return false;
    }
    const int64_t elapsed = MeasureUs([&] { std::memcpy(destination, expected.data(), expected.size()); });
    Log(tid, "[HOST_COPY] phase=create_buffer size=", expected.size(), " elapsed_us=", elapsed);
    return true;
}

bool RunSetOne(KVClient &client, const Options &options, const std::string &key, int tid, Summary &summary)
{
    if (options.cleanupBefore) {
        DeleteKey(client, key, tid);
    }
    const auto expected = MakeExpectedData(key, options.valueSize);
    std::shared_ptr<datasystem::Buffer> buffer;
    Status rc;
    const int64_t createUs = MeasureUs([&] { rc = client.Create(key, expected.size(), SetParam{}, buffer); });
    Log(tid, "[CREATE] key=", key, " rc=", rc.ToString(), " elapsed_us=", createUs);
    if (rc.IsError() || buffer == nullptr) {
        return false;
    }
    ++summary.createOk;
    if (!CopyExpectedToCreateBuffer(expected, buffer, tid)) {
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
    connect.deviceId = std::to_string(options.gpuId);
    if (!options.etcdAddress.empty()) {
        ServiceDiscoveryOptions discoveryOptions;
        discoveryOptions.etcdAddress = options.etcdAddress;
        discoveryOptions.clusterName = options.clusterName;
        discoveryOptions.hostIdEnvName = options.hostIdEnvName;
        discoveryOptions.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<ServiceDiscovery>(discoveryOptions);
        Status discoveryRc = serviceDiscovery->Init();
        if (discoveryRc.IsError()) {
            std::cerr << "[DISCOVERY_INIT_FAIL] " << discoveryRc.ToString() << std::endl;
            return nullptr;
        }
        connect.enableLocalCache = false;
        connect.enableCrossNodeConnection = true;
        connect.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        connect.serviceDiscovery = std::move(serviceDiscovery);
        std::cout << "[DISCOVERY_INIT] rc=OK etcd_address=" << options.etcdAddress
                  << " cluster_name=" << options.clusterName
                  << " host_id_env_name=" << (options.hostIdEnvName.empty() ? "<empty>" : options.hostIdEnvName)
                  << std::endl;
        std::cout << "[CLIENT_CONFIG] enable_local_cache=false enable_cross_node_connection=true "
                  << "data_placement_policy=PREFERRED_META_OWNER" << std::endl;
    } else {
        connect.host = options.host;
        connect.port = options.port;
        connect.enableLocalCache = options.enableLocalCache;
    }
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
              << "connection mode   : " << (options.etcdAddress.empty() ? "direct worker" : "service discovery")
              << '\n'
              << "service endpoint  : "
              << (options.etcdAddress.empty() ? options.host : options.etcdAddress) << '\n'
              << "cluster name      : "
              << (options.etcdAddress.empty() ? "N/A" : options.clusterName) << '\n'
              << "enable local cache: " << std::boolalpha
              << (options.etcdAddress.empty() ? options.enableLocalCache : false) << '\n'
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

struct PendingBuffer {
    std::shared_ptr<datasystem::Buffer> buffer;
    uint64_t size = 0;
};

using PendingBufferMap = std::unordered_map<std::string, PendingBuffer>;
using PendingBatchMap = std::unordered_map<std::string, std::vector<std::string>>;

void PrintShellHelp()
{
    std::cout << "Commands:\n"
              << "  create <key> [size]       Create and fill a Buffer, but do not Set it\n"
              << "  set <key>                 Set a pending Buffer\n"
              << "  get <key> [size]          Get and verify through GPU\n"
              << "  roundtrip <key> [size]    Create, Set, Get and verify\n"
              << "  mcreate <prefix> <count> [size]  MCreate and keep pending Buffers\n"
              << "  mset <prefix>             MSet a pending batch\n"
              << "  mget <prefix> <count> [size]  Batch Get and verify through GPU\n"
              << "  mroundtrip <prefix> <count> [size]  MCreate, MSet and MGet\n"
              << "  parallel <op> <prefix> <count> <threads> [size]\n"
              << "                             op is set, get or roundtrip\n"
              << "  del <key>                 Delete a published value\n"
              << "  discard <key>             Release a pending Buffer without Set\n"
              << "  pending                   List pending Buffers\n"
              << "  sleep <ms>                Wait while keeping the client alive\n"
              << "  status                    Print cumulative counters\n"
              << "  help                      Print this help\n"
              << "  quit                      Exit\n";
}

bool ParseCommandSize(std::istringstream &stream, uint64_t defaultSize, uint64_t &size)
{
    std::string value;
    if (!(stream >> value)) {
        size = defaultSize;
        return true;
    }
    size = std::stoull(value);
    return size > 0;
}

bool CreatePendingBuffer(KVClient &client, const Options &options, const std::string &key, uint64_t size,
                         PendingBufferMap &pendingBuffers, Summary &summary)
{
    if (pendingBuffers.count(key) != 0) {
        Log(0, "[CREATE_FAIL] key=", key, " already has a Buffer waiting for Set");
        return false;
    }
    if (options.cleanupBefore) {
        DeleteKey(client, key, 0);
    }
    const auto expected = MakeExpectedData(key, size);
    std::shared_ptr<datasystem::Buffer> buffer;
    Status rc;
    const int64_t createUs = MeasureUs([&] { rc = client.Create(key, size, SetParam{}, buffer); });
    Log(0, "[CREATE] key=", key, " size=", size, " rc=", rc.ToString(), " elapsed_us=", createUs);
    if (rc.IsError() || buffer == nullptr) {
        return false;
    }
    ++summary.createOk;
    if (!CopyExpectedToCreateBuffer(expected, buffer, 0)) {
        return false;
    }
    pendingBuffers.emplace(key, PendingBuffer{ std::move(buffer), size });
    Log(0, "[PENDING] key=", key, " is waiting for Set");
    return true;
}

bool SetPendingBuffer(KVClient &client, const std::string &key, PendingBufferMap &pendingBuffers, Summary &summary)
{
    auto entry = pendingBuffers.find(key);
    if (entry == pendingBuffers.end()) {
        Log(0, "[SET_FAIL] key=", key, " has no pending Create Buffer");
        return false;
    }
    Status rc;
    const int64_t setUs = MeasureUs([&] { rc = client.Set(entry->second.buffer); });
    Log(0, "[SET] key=", key, " rc=", rc.ToString(), " elapsed_us=", setUs);
    if (rc.IsError()) {
        Log(0, "[PENDING] Set failed; Buffer is retained for retry, key=", key);
        return false;
    }
    ++summary.setOk;
    pendingBuffers.erase(entry);
    return true;
}

void PrintPendingBuffers(const PendingBufferMap &pendingBuffers)
{
    std::cout << "[PENDING] count=" << pendingBuffers.size() << std::endl;
    for (const auto &entry : pendingBuffers) {
        std::cout << "  key=" << entry.first << " size=" << entry.second.size << std::endl;
    }
}

void PrintPendingBatches(const PendingBatchMap &pendingBatches)
{
    std::cout << "[PENDING_BATCH] count=" << pendingBatches.size() << std::endl;
    for (const auto &entry : pendingBatches) {
        std::cout << "  prefix=" << entry.first << " buffers=" << entry.second.size() << std::endl;
    }
}

bool ReadKeyAndSize(std::istringstream &stream, const Options &options, std::string &key, uint64_t &size)
{
    return static_cast<bool>(stream >> key) && ParseCommandSize(stream, options.valueSize, size);
}

bool RunShellDataCommand(const std::string &command, std::istringstream &stream, KVClient &client,
                         const Options &options, PendingBufferMap &pendingBuffers, Summary &summary)
{
    std::string key;
    uint64_t size = 0;
    if (command == "set") {
        return static_cast<bool>(stream >> key) && SetPendingBuffer(client, key, pendingBuffers, summary);
    }
    if (!ReadKeyAndSize(stream, options, key, size)) {
        Log(0, "[COMMAND_ERROR] usage: ", command, " <key> [size]");
        return false;
    }
    if (command == "create") {
        return CreatePendingBuffer(client, options, key, size, pendingBuffers, summary);
    }
    Options commandOptions = options;
    commandOptions.valueSize = size;
    if (command == "get") {
        return RunGetOne(client, commandOptions, key, 0, summary);
    }
    return RunSetOne(client, commandOptions, key, 0, summary)
           && RunGetOne(client, commandOptions, key, 0, summary);
}

bool RunShellControlCommand(const std::string &command, std::istringstream &stream, KVClient &client,
                            PendingBufferMap &pendingBuffers)
{
    if (command == "pending") {
        PrintPendingBuffers(pendingBuffers);
        return true;
    }
    std::string key;
    if (command == "del") {
        if (!(stream >> key)) {
            Log(0, "[COMMAND_ERROR] usage: del <key>");
            return false;
        }
        DeleteKey(client, key, 0);
        return true;
    }
    if (command == "discard") {
        if (!(stream >> key)) {
            Log(0, "[COMMAND_ERROR] usage: discard <key>");
            return false;
        }
        const size_t erased = pendingBuffers.erase(key);
        Log(0, "[DISCARD] key=", key, " erased=", erased);
        return erased != 0;
    }
    uint64_t waitMs = 0;
    if (!(stream >> waitMs)) {
        Log(0, "[COMMAND_ERROR] usage: sleep <ms>");
        return false;
    }
    Log(0, "[SLEEP] begin, ms=", waitMs);
    std::this_thread::sleep_for(std::chrono::milliseconds(waitMs));
    Log(0, "[SLEEP] end, ms=", waitMs);
    return true;
}

std::vector<std::string> BuildBatchKeys(const std::string &prefix, int count)
{
    std::vector<std::string> keys;
    keys.reserve(count);
    for (int index = 0; index < count; ++index) {
        keys.emplace_back(prefix + "_" + std::to_string(index));
    }
    return keys;
}

bool ParseBatchArgs(std::istringstream &stream, const Options &options, std::string &prefix, int &count,
                    uint64_t &size)
{
    return static_cast<bool>(stream >> prefix >> count) && count > 0
           && ParseCommandSize(stream, options.valueSize, size);
}

bool FillBatchBuffers(const std::vector<std::string> &keys,
                      const std::vector<std::shared_ptr<datasystem::Buffer>> &buffers, uint64_t size)
{
    if (keys.size() != buffers.size()) {
        Log(0, "[MCREATE_FAIL] returned Buffer count mismatch: keys=", keys.size(), " buffers=", buffers.size());
        return false;
    }
    for (size_t index = 0; index < keys.size(); ++index) {
        if (buffers[index] == nullptr || buffers[index]->GetSize() != static_cast<int64_t>(size)) {
            Log(0, "[MCREATE_FAIL] invalid Buffer, key=", keys[index]);
            return false;
        }
        const auto expected = MakeExpectedData(keys[index], size);
        if (!CopyExpectedToCreateBuffer(expected, buffers[index], 0)) {
            return false;
        }
    }
    return true;
}

bool MCreatePending(KVClient &client, const Options &options, const std::string &prefix, int count, uint64_t size,
                    PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches, Summary &summary)
{
    if (pendingBatches.count(prefix) != 0) {
        Log(0, "[MCREATE_FAIL] prefix=", prefix, " already has a pending batch");
        return false;
    }
    auto keys = BuildBatchKeys(prefix, count);
    for (const auto &key : keys) {
        if (pendingBuffers.count(key) != 0) {
            Log(0, "[MCREATE_FAIL] key=", key, " already has a pending Buffer");
            return false;
        }
        if (options.cleanupBefore) {
            DeleteKey(client, key, 0);
        }
    }
    std::vector<std::shared_ptr<datasystem::Buffer>> buffers;
    std::vector<uint64_t> sizes(keys.size(), size);
    Status rc;
    const int64_t elapsed = MeasureUs([&] { rc = client.MCreate(keys, sizes, SetParam{}, buffers); });
    Log(0, "[MCREATE] prefix=", prefix, " count=", count, " rc=", rc.ToString(), " elapsed_us=", elapsed);
    if (rc.IsError() || !FillBatchBuffers(keys, buffers, size)) {
        return false;
    }
    summary.createOk += count;
    for (size_t index = 0; index < keys.size(); ++index) {
        pendingBuffers.emplace(keys[index], PendingBuffer{ std::move(buffers[index]), size });
    }
    pendingBatches.emplace(prefix, std::move(keys));
    return true;
}

bool MSetPending(KVClient &client, const std::string &prefix, PendingBufferMap &pendingBuffers,
                 PendingBatchMap &pendingBatches, Summary &summary)
{
    auto batch = pendingBatches.find(prefix);
    if (batch == pendingBatches.end()) {
        Log(0, "[MSET_FAIL] prefix=", prefix, " has no pending MCreate batch");
        return false;
    }
    std::vector<std::shared_ptr<datasystem::Buffer>> buffers;
    buffers.reserve(batch->second.size());
    for (const auto &key : batch->second) {
        auto entry = pendingBuffers.find(key);
        if (entry == pendingBuffers.end()) {
            Log(0, "[MSET_FAIL] missing pending Buffer, key=", key);
            return false;
        }
        buffers.emplace_back(entry->second.buffer);
    }
    Status rc;
    const int64_t elapsed = MeasureUs([&] { rc = client.MSet(buffers); });
    Log(0, "[MSET] prefix=", prefix, " count=", buffers.size(), " rc=", rc.ToString(), " elapsed_us=", elapsed);
    if (rc.IsError()) {
        return false;
    }
    summary.setOk += buffers.size();
    for (const auto &key : batch->second) {
        pendingBuffers.erase(key);
    }
    pendingBatches.erase(batch);
    return true;
}

bool VerifyBatchGet(const std::vector<std::string> &keys, std::vector<Optional<ReadOnlyBuffer>> &buffers,
                    uint64_t size, Summary &summary)
{
    if (keys.size() != buffers.size()) {
        Log(0, "[MGET_FAIL] returned Buffer count mismatch: keys=", keys.size(), " buffers=", buffers.size());
        return false;
    }
    for (size_t index = 0; index < keys.size(); ++index) {
        if (!buffers[index] || buffers[index]->GetSize() != static_cast<int64_t>(size)) {
            Log(0, "[MGET_FAIL] missing or invalid Buffer, key=", keys[index]);
            return false;
        }
        ++summary.getOk;
        const auto expected = MakeExpectedData(keys[index], size);
        std::vector<uint8_t> actual(size);
        if (!CopyReadBufferThroughGpu(*buffers[index], actual, 0, summary) || actual != expected) {
            Log(0, "[VERIFY_FAIL] key=", keys[index], " data mismatch");
            return false;
        }
        ++summary.verifyOk;
    }
    return true;
}

bool MGetBatch(KVClient &client, const Options &options, const std::string &prefix, int count, uint64_t size,
               Summary &summary)
{
    const auto keys = BuildBatchKeys(prefix, count);
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    Status rc;
    const int64_t elapsed = MeasureUs([&] { rc = client.Get(keys, buffers, options.timeoutMs); });
    Log(0, "[MGET] prefix=", prefix, " count=", count, " rc=", rc.ToString(), " elapsed_us=", elapsed);
    return rc.IsOk() && VerifyBatchGet(keys, buffers, size, summary);
}

void RunParallelWorker(const std::shared_ptr<KVClient> &client, const Options &options, const std::string &operation,
                       const std::string &prefix, int count, int tid, std::atomic<int> &next, Summary &summary)
{
    if (!CheckCuda(cudaSetDevice(options.gpuId), tid, "cudaSetDevice in parallel worker")) {
        ++summary.failed;
        return;
    }
    for (int index = next.fetch_add(1); index < count; index = next.fetch_add(1)) {
        const std::string key = prefix + "_" + std::to_string(index);
        Options commandOptions = options;
        commandOptions.command = operation;
        (void)RunOne(*client, commandOptions, key, tid, summary);
    }
}

bool RunParallel(std::istringstream &stream, const std::shared_ptr<KVClient> &client, const Options &options,
                 Summary &summary)
{
    std::string operation;
    std::string prefix;
    int count = 0;
    int threadNum = 0;
    uint64_t size = 0;
    if (!(stream >> operation >> prefix >> count >> threadNum) || count <= 0 || threadNum <= 0
        || !ParseCommandSize(stream, options.valueSize, size)
        || (operation != "set" && operation != "get" && operation != "roundtrip")) {
        Log(0, "[COMMAND_ERROR] usage: parallel <set|get|roundtrip> <prefix> <count> <threads> [size]");
        return false;
    }
    Options commandOptions = options;
    commandOptions.valueSize = size;
    std::atomic<int> next{ 0 };
    std::vector<std::thread> threads;
    for (int index = 0; index < threadNum; ++index) {
        threads.emplace_back(RunParallelWorker, client, std::cref(commandOptions), std::cref(operation),
                             std::cref(prefix), count, index, std::ref(next), std::ref(summary));
    }
    for (auto &thread : threads) {
        thread.join();
    }
    return true;
}

bool RunShellBatchCommand(const std::string &command, std::istringstream &stream, KVClient &client,
                          const Options &options, PendingBufferMap &pendingBuffers,
                          PendingBatchMap &pendingBatches, Summary &summary)
{
    std::string prefix;
    if (command == "mset") {
        if (!(stream >> prefix)) {
            Log(0, "[COMMAND_ERROR] usage: mset <prefix>");
            return false;
        }
        return MSetPending(client, prefix, pendingBuffers, pendingBatches, summary);
    }
    int count = 0;
    uint64_t size = 0;
    if (!ParseBatchArgs(stream, options, prefix, count, size)) {
        Log(0, "[COMMAND_ERROR] usage: ", command, " <prefix> <count> [size]");
        return false;
    }
    if (command == "mcreate") {
        return MCreatePending(client, options, prefix, count, size, pendingBuffers, pendingBatches, summary);
    }
    if (command == "mget") {
        return MGetBatch(client, options, prefix, count, size, summary);
    }
    return MCreatePending(client, options, prefix, count, size, pendingBuffers, pendingBatches, summary)
           && MSetPending(client, prefix, pendingBuffers, pendingBatches, summary)
           && MGetBatch(client, options, prefix, count, size, summary);
}

bool IsBatchCommand(const std::string &command)
{
    return command == "mcreate" || command == "mset" || command == "mget" || command == "mroundtrip";
}

bool ExecuteShellCommand(const std::string &command, std::istringstream &stream,
                         const std::shared_ptr<KVClient> &client, const Options &options,
                         PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches, Summary &summary,
                         int64_t initUs)
{
    if (command == "help") {
        PrintShellHelp();
        return true;
    }
    if (command == "status") {
        PrintSummary(options, summary, initUs);
        PrintPendingBuffers(pendingBuffers);
        PrintPendingBatches(pendingBatches);
        return true;
    }
    if (command == "create" || command == "set" || command == "get" || command == "roundtrip") {
        return RunShellDataCommand(command, stream, *client, options, pendingBuffers, summary);
    }
    if (IsBatchCommand(command)) {
        return RunShellBatchCommand(command, stream, *client, options, pendingBuffers, pendingBatches, summary);
    }
    if (command == "parallel") {
        return RunParallel(stream, client, options, summary);
    }
    if (command == "del" || command == "discard" || command == "pending" || command == "sleep") {
        return RunShellControlCommand(command, stream, *client, pendingBuffers);
    }
    Log(0, "[COMMAND_ERROR] unknown command: ", command, "; enter help for usage");
    return false;
}

int RunShell(const Options &options)
{
    int64_t initUs = 0;
    auto client = InitClient(options, initUs);
    if (client == nullptr) {
        return 1;
    }
    if (!CheckCuda(cudaSetDevice(options.gpuId), 0, "cudaSetDevice")) {
        return 1;
    }
    std::cout << "[INIT] rc=OK elapsed_us=" << initUs << " client=" << client.get() << std::endl;
    PrintShellHelp();
    Summary summary;
    PendingBufferMap pendingBuffers;
    PendingBatchMap pendingBatches;
    std::string line;
    while (std::cout << "async-pin> " && std::getline(std::cin, line)) {
        std::istringstream stream(line);
        std::string command;
        if (!(stream >> command)) {
            continue;
        }
        if (command == "quit" || command == "exit") {
            break;
        }
        try {
            const bool success = ExecuteShellCommand(command, stream, client, options, pendingBuffers,
                                                     pendingBatches, summary, initUs);
            if (!success) {
                ++summary.failed;
            }
        } catch (const std::exception &error) {
            ++summary.failed;
            Log(0, "[COMMAND_ERROR] ", error.what());
        }
    }
    PrintSummary(options, summary, initUs);
    PrintPendingBuffers(pendingBuffers);
    PrintPendingBatches(pendingBatches);
    std::cout << "[EXIT] releasing pending Buffers and destroying KVClient" << std::endl;
    return summary.failed.load() == 0 ? 0 : 2;
}

int Run(const Options &options)
{
    if (options.command == "shell") {
        return RunShell(options);
    }
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

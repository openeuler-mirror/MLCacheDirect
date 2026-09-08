#include "datasystem/kv_client.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/service_discovery.h"

#include <cuda_runtime.h>

#include <atomic>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <deque>
#include <functional>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

using datasystem::ConnectOptions;
using datasystem::CoordinatorServiceDiscovery;
using datasystem::CoordinatorServiceDiscoveryOptions;
using datasystem::DataPlacementPolicy;
using datasystem::KVClient;
using datasystem::Optional;
using datasystem::ReadOnlyBuffer;
using datasystem::ServiceAffinityPolicy;
using datasystem::ServiceDiscovery;
using datasystem::ServiceDiscoveryOptions;
using datasystem::SetParam;
using datasystem::Status;

static int CudaMemcpyAsyncAdapter(void *dst, const void *src, size_t size, datasystem::DsCudaMemcpyKind kind,
                                  void *stream)
{
    cudaMemcpyKind cudaKind;
    switch (kind) {
        case datasystem::DsCudaMemcpyKind::HOST_TO_DEVICE:
            cudaKind = cudaMemcpyHostToDevice;
            break;
        case datasystem::DsCudaMemcpyKind::DEVICE_TO_HOST:
            cudaKind = cudaMemcpyDeviceToHost;
            break;
        default:
            return static_cast<int>(cudaErrorInvalidValue);
    }
    return static_cast<int>(
        cudaMemcpyAsync(dst, src, size, cudaKind, reinterpret_cast<cudaStream_t>(stream)));
}

static void CudaRegisterPinFuncs()
{
    datasystem::CudaFuncs funcs;
    funcs.hostRegister = reinterpret_cast<datasystem::HostRegisterFunc>(cudaHostRegister);
    funcs.hostUnregister = reinterpret_cast<datasystem::HostUnregisterFunc>(cudaHostUnregister);
    funcs.getErrorString = reinterpret_cast<datasystem::GetErrorStringFunc>(cudaGetErrorString);
    funcs.memcpyAsync = CudaMemcpyAsyncAdapter;
    KVClient::RegisterCudaFuncs(funcs);
}

namespace {

std::mutex g_logMutex;

struct Options {
    std::string host;
    std::string command;
    std::string coordinatorAddress;
    std::string etcdAddress;
    std::string clusterName;
    std::string hostIdEnvName = "JDOS_HOST_IP";
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
    bool pin = true;
    bool verify = false;
    bool help = false;
};

struct Summary {
    std::atomic<int64_t> createOk{ 0 };
    std::atomic<int64_t> setOk{ 0 };
    std::atomic<int64_t> getOk{ 0 };
    std::atomic<int64_t> h2dOk{ 0 };
    std::atomic<int64_t> d2hOk{ 0 };
    std::atomic<int64_t> verifyOk{ 0 };
    std::atomic<int64_t> failed{ 0 };
};

struct LatencySamples {
    std::mutex mutex;
    std::unordered_map<std::string, std::vector<int64_t>> values;
    std::unordered_map<std::string, int64_t> failures;

    void Add(const std::string &name, int64_t elapsedUs)
    {
        std::lock_guard<std::mutex> lock(mutex);
        values[name].emplace_back(elapsedUs);
    }

    void Fail(const std::string &name)
    {
        std::lock_guard<std::mutex> lock(mutex);
        ++failures[name];
    }
};

struct TransferTiming {
    int64_t enqueueUs = 0;
    int64_t syncUs = 0;
    int64_t totalUs = 0;
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

class CudaStream {
public:
    ~CudaStream()
    {
        if (stream_ != nullptr) {
            (void)cudaStreamDestroy(stream_);
        }
    }

    cudaError_t Create()
    {
        return cudaStreamCreateWithFlags(&stream_, cudaStreamNonBlocking);
    }

    cudaError_t Synchronize() const
    {
        return cudaStreamSynchronize(stream_);
    }

    void *Data() const
    {
        return reinterpret_cast<void *>(stream_);
    }

private:
    cudaStream_t stream_ = nullptr;
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
    if (name == "pin") {
        return ParseBool(value, options.pin);
    }
    if (name == "cleanup_before") {
        return ParseBool(value, options.cleanupBefore);
    }
    if (name == "delete_after") {
        return ParseBool(value, options.deleteAfter);
    }
    if (name == "verify") {
        return ParseBool(value, options.verify);
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
    } else if (name == "coordinator_address") {
        options.coordinatorAddress = value;
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
    if (first == "create_set" || first == "get" || first == "roundtrip" || first == "shell") {
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
    const bool validCommand = options.command == "create_set" || options.command == "get"
                              || options.command == "roundtrip" || options.command == "shell";
    const bool hasCoordinator = !options.coordinatorAddress.empty();
    const bool hasEtcd = !options.etcdAddress.empty();
    if (hasCoordinator && hasEtcd) {
        std::cerr << "--coordinator_address and --etcd_address cannot be configured together" << std::endl;
        return false;
    }
    const bool hasServiceDiscovery = hasCoordinator || hasEtcd;
    if (hasEtcd && options.clusterName.empty()) {
        std::cerr << "--cluster_name is required with --etcd_address" << std::endl;
        return false;
    }
    const bool hasOrphanClusterName = !options.clusterName.empty() && !hasServiceDiscovery;
    const bool validConnection = !hasOrphanClusterName
                                 && (hasServiceDiscovery
                                         || (!options.host.empty() && options.port > 0 && options.port <= 65535));
    if (!validCommand || !validConnection) {
        return false;
    }
    return options.count > 0 && options.valueSize > 0 && options.threadNum > 0 && options.gpuId >= 0
           && options.timeoutMs > 0;
}

void PrintUsage(const char *program)
{
    std::cout << "Usage:\n"
              << "  " << program << " <host> <create_set|get|roundtrip|shell> [options]\n"
              << "  " << program
              << " <create_set|get|roundtrip|shell> --coordinator_address=ADDR [--cluster_name=NAME] [options]\n"
              << "  " << program
              << " <create_set|get|roundtrip|shell> --etcd_address=ADDR --cluster_name=NAME [options]\n"
              << "  --port=N                 Worker port, default 18481\n"
              << "  --coordinator_address=ADDR  Coordinator address list for service discovery\n"
              << "  --etcd_address=ADDR      ETCD address list for service discovery\n"
              << "  --cluster_name=NAME      Optional Datasystem cluster namespace\n"
              << "  --host_id_env_name=NAME  Environment variable containing the local host ID, default JDOS_HOST_IP\n"
              << "  --count=N                Keys per thread, default 1\n"
              << "  --value_size=N           Bytes per value, default 3670016\n"
              << "  --key_prefix=TEXT        Key prefix, default async_pin\n"
              << "  --gpu_id=N               CUDA device, default 0\n"
              << "  --thread=N               Concurrent threads, default 1\n"
              << "  --timeout_ms=N           Get timeout, default 60000\n"
              << "  --enable_local_cache=B   Local-cache mode, default true\n"
              << "  --pin=B                  Register CUDA host-memory funcs before KVClient Init, default true\n"
              << "  --cleanup_before=B       Delete keys before Set, default true\n"
              << "  --delete_after=B         Delete keys after test, default false\n"
              << "  --verify=B               Verify copied data, default false\n"
              << "\nShell commands:\n"
              << "  create_set <key> [size]   Create, D2H and Set a Buffer\n"
              << "  get <key> [size]          Get a Buffer and complete Buffer-to-GPU H2D\n"
              << "  roundtrip <key> [size]    Create, D2H, Set, Get and H2D\n"
              << "  mcreate_set <prefix> <count> [size]  MCreate, batched D2H and MSet\n"
              << "  mget <prefix> <count> [size]  Batch Get and complete batched H2D\n"
              << "  mroundtrip <prefix> <count> [size]  MCreate, D2H, MSet, MGet and H2D\n"
              << "  parallel <op> <prefix> <request_count> <threads> [size] [batch_size]\n"
              << "  qps <op> <prefix> <qps> <time_seconds> <threads> [size] [batch_size]\n"
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

bool CheckCuda(cudaError_t error, int tid, const std::string &operation)
{
    if (error == cudaSuccess) {
        return true;
    }
    Log(tid, "[CUDA_ERROR] operation=", operation, " detail=", cudaGetErrorString(error));
    return false;
}

struct GpuResources {
    std::shared_ptr<CudaStream> stream;
    std::vector<std::unique_ptr<DeviceBuffer>> sourceBuffers;
    std::vector<std::unique_ptr<DeviceBuffer>> destinationBuffers;

    bool Initialize(size_t size, size_t count, int gpuId, int tid)
    {
        auto sharedStream = std::make_shared<CudaStream>();
        if (!CheckCuda(cudaSetDevice(gpuId), tid, "cudaSetDevice")
            || !CheckCuda(sharedStream->Create(), tid, "cudaStreamCreateWithFlags")) {
            return false;
        }
        return InitializeWithStream(size, count, sharedStream, tid);
    }

    bool InitializeWithStream(size_t size, size_t count, const std::shared_ptr<CudaStream> &sharedStream, int tid)
    {
        if (sharedStream == nullptr) {
            return false;
        }
        stream = sharedStream;
        sourceBuffers.reserve(count);
        destinationBuffers.reserve(count);
        for (size_t index = 0; index < count; ++index) {
            auto source = std::make_unique<DeviceBuffer>();
            auto destination = std::make_unique<DeviceBuffer>();
            if (!CheckCuda(source->Allocate(size), tid, "cudaMalloc source")
                || !CheckCuda(destination->Allocate(size), tid, "cudaMalloc destination")
                || !CheckCuda(cudaMemsetAsync(source->Data(), 0x5a, size,
                                              reinterpret_cast<cudaStream_t>(stream->Data())),
                              tid, "cudaMemsetAsync source")) {
                return false;
            }
            sourceBuffers.emplace_back(std::move(source));
            destinationBuffers.emplace_back(std::move(destination));
        }
        return CheckCuda(stream->Synchronize(), tid, "GPU resource initialization synchronize");
    }
};

void DeleteKey(KVClient &client, const std::string &key, int tid)
{
    std::vector<std::string> failedKeys;
    Status rc = client.Del({ key }, failedKeys);
    if (rc.IsError() && !failedKeys.empty()) {
        Log(tid, "[DELETE_WARN] key=", key, " rc=", rc.ToString());
    }
}

std::shared_ptr<KVClient> InitClient(const Options &options, int64_t &elapsedUs)
{
    ConnectOptions connect;
    connect.deviceId = std::to_string(options.gpuId);
    if (!options.coordinatorAddress.empty()) {
        CoordinatorServiceDiscoveryOptions discoveryOptions;
        discoveryOptions.serviceAddress = options.coordinatorAddress;
        discoveryOptions.clusterName = options.clusterName;
        discoveryOptions.hostIdEnvName = options.hostIdEnvName;
        discoveryOptions.affinityPolicy = ServiceAffinityPolicy::PREFERRED_SAME_NODE;
        auto serviceDiscovery = std::make_shared<CoordinatorServiceDiscovery>(discoveryOptions);
        Status discoveryRc = serviceDiscovery->Init();
        if (discoveryRc.IsError()) {
            std::cerr << "[DISCOVERY_INIT_FAIL] " << discoveryRc.ToString() << std::endl;
            return nullptr;
        }
        connect.connectTimeoutMs = 1000;
        connect.requestTimeoutMs = 60000;
        connect.enableLocalCache = false;
        connect.enableCrossNodeConnection = true;
        connect.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        connect.fastTransportMemSize = 512ULL * 1024 * 1024;
        connect.serviceDiscovery = std::move(serviceDiscovery);
        std::cout << "[DISCOVERY_INIT] rc=OK coordinator_address=" << options.coordinatorAddress
                  << " cluster_name=" << options.clusterName
                  << " host_id_env_name=" << (options.hostIdEnvName.empty() ? "<empty>" : options.hostIdEnvName)
                  << std::endl;
        std::cout << "[CLIENT_CONFIG] connect_timeout_ms=1000 request_timeout_ms=20 "
                  << "enable_local_cache=false enable_cross_node_connection=true "
                  << "data_placement_policy=PREFERRED_META_OWNER fast_transport_mem_size=536870912" << std::endl;
    } else if (!options.etcdAddress.empty()) {
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
        connect.connectTimeoutMs = 1000;
        connect.requestTimeoutMs = 60000;
        connect.enableLocalCache = false;
        connect.enableCrossNodeConnection = true;
        connect.dataPlacementPolicy = DataPlacementPolicy::PREFERRED_META_OWNER;
        connect.fastTransportMemSize = 512ULL * 1024 * 1024;
        connect.serviceDiscovery = std::move(serviceDiscovery);
        std::cout << "[DISCOVERY_INIT] rc=OK etcd_address=" << options.etcdAddress
                  << " cluster_name=" << options.clusterName
                  << " host_id_env_name=" << (options.hostIdEnvName.empty() ? "<empty>" : options.hostIdEnvName)
                  << std::endl;
        std::cout << "[CLIENT_CONFIG] connect_timeout_ms=1000 request_timeout_ms=20 "
                  << "enable_local_cache=false enable_cross_node_connection=true "
                  << "data_placement_policy=PREFERRED_META_OWNER fast_transport_mem_size=536870912" << std::endl;
    } else {
        connect.host = options.host;
        connect.port = options.port;
        connect.enableLocalCache = options.enableLocalCache;
    }
    auto client = std::make_shared<KVClient>(connect);
    if (options.pin) {
        CudaRegisterPinFuncs();
    }
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
              << "connection mode   : "
              << (!options.coordinatorAddress.empty()
                      ? "coordinator service discovery"
                      : (!options.etcdAddress.empty() ? "etcd service discovery" : "direct worker"))
              << '\n'
              << "service endpoint  : "
              << (!options.coordinatorAddress.empty()
                      ? options.coordinatorAddress
                      : (!options.etcdAddress.empty() ? options.etcdAddress : options.host))
              << '\n'
              << "cluster name      : "
              << (options.coordinatorAddress.empty() && options.etcdAddress.empty() ? "N/A" : options.clusterName)
              << '\n'
              << "enable local cache: " << std::boolalpha
              << (options.coordinatorAddress.empty() && options.etcdAddress.empty() ? options.enableLocalCache : false)
              << '\n'
              << "pin               : " << std::boolalpha << options.pin << '\n'
              << "verify            : " << std::boolalpha << options.verify << '\n'
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

std::mutex g_pendingMutex;

void AddLatency(LatencySamples *samples, const std::string &stage, int64_t elapsedUs)
{
    if (samples != nullptr) {
        samples->Add(stage, elapsedUs);
    }
}

void AddStageFailure(LatencySamples *samples, const std::string &stage)
{
    if (samples != nullptr) {
        samples->Fail(stage);
    }
}

bool InitializeGpuResources(GpuResources &resources, const Options &options, size_t count, uint64_t size, int tid)
{
    return resources.Initialize(size, count, options.gpuId, tid);
}

bool InitializeGpuResourcePool(std::vector<GpuResources> &resourcePool, const Options &options,
                               int threadNum, size_t count, uint64_t size)
{
    auto sharedStream = std::make_shared<CudaStream>();
    if (!CheckCuda(cudaSetDevice(options.gpuId), 0, "cudaSetDevice")
        || !CheckCuda(sharedStream->Create(), 0, "cudaStreamCreateWithFlags")) {
        return false;
    }
    resourcePool.resize(static_cast<size_t>(threadNum));
    for (int tid = 0; tid < threadNum; ++tid) {
        if (!resourcePool[static_cast<size_t>(tid)].InitializeWithStream(size, count, sharedStream, tid)) {
            return false;
        }
    }
    Log(0, "[CUDA_INIT] shared_stream=", sharedStream->Data(), " threads=", threadNum,
        " buffers_per_thread=", count, " buffer_size=", size);
    return true;
}

bool PrepareGpuSources(GpuResources &resources, const std::vector<std::string> &keys, uint64_t size, bool verify,
                       int tid, std::vector<std::vector<uint8_t>> &expected)
{
    expected.clear();
    if (!verify) {
        return true;
    }
    expected.reserve(keys.size());
    for (size_t index = 0; index < keys.size(); ++index) {
        expected.emplace_back(MakeExpectedData(keys[index], size));
        if (!CheckCuda(cudaMemcpyAsync(resources.sourceBuffers[index]->Data(), expected.back().data(), size,
                                       cudaMemcpyHostToDevice,
                                       reinterpret_cast<cudaStream_t>(resources.stream->Data())),
                       tid, "prepare verification source H2D")) {
            return false;
        }
    }
    return CheckCuda(resources.stream->Synchronize(), tid, "prepare verification source synchronize");
}

bool RunDsTransfer(KVClient &client, GpuResources &resources, const std::vector<void *> &hostAddresses,
                   const std::vector<uint64_t> &sizes, bool d2h, int tid, const std::string &phase,
                   LatencySamples *samples, bool quiet, TransferTiming &timing)
{
    if (hostAddresses.size() != sizes.size() || hostAddresses.size() > resources.sourceBuffers.size()) {
        AddStageFailure(samples, d2h ? "d2h" : "h2d");
        Log(tid, "[CUDA_ERROR] operation=", phase, " invalid transfer arguments");
        return false;
    }
    Status copyStatus;
    const auto totalBegin = std::chrono::steady_clock::now();
    timing.enqueueUs = MeasureUs([&] {
        for (size_t index = 0; index < hostAddresses.size(); ++index) {
            void *dst = d2h ? hostAddresses[index] : resources.destinationBuffers[index]->Data();
            const void *src = d2h ? resources.sourceBuffers[index]->Data() : hostAddresses[index];
            copyStatus = client.DsCudaMemcpyAsync(
                dst, src, sizes[index],
                d2h ? datasystem::DsCudaMemcpyKind::DEVICE_TO_HOST : datasystem::DsCudaMemcpyKind::HOST_TO_DEVICE,
                resources.stream->Data());
            if (copyStatus.IsError()) {
                break;
            }
        }
    });
    cudaError_t syncError = cudaSuccess;
    timing.syncUs = MeasureUs([&] { syncError = resources.stream->Synchronize(); });
    timing.totalUs = std::chrono::duration_cast<std::chrono::microseconds>(
                         std::chrono::steady_clock::now() - totalBegin)
                         .count();
    if (copyStatus.IsError()) {
        AddStageFailure(samples, d2h ? "d2h" : "h2d");
        Log(tid, "[CUDA_ERROR] operation=", phase, " detail=", copyStatus.ToString());
        return false;
    }
    if (!CheckCuda(syncError, tid, phase + " synchronize")) {
        AddStageFailure(samples, d2h ? "d2h" : "h2d");
        return false;
    }
    const std::string stage = d2h ? "d2h" : "h2d";
    AddLatency(samples, stage, timing.totalUs);
    if (!quiet) {
        Log(tid, "[CUDA_COPY] phase=", phase, " direction=", d2h ? "D2H" : "H2D",
            " buffers=", hostAddresses.size(), " enqueue_us=", timing.enqueueUs, " sync_us=", timing.syncUs,
            " total_us=", timing.totalUs);
    }
    return true;
}

bool VerifyCreatedBuffers(const std::vector<void *> &addresses,
                          const std::vector<std::vector<uint8_t>> &expected, const std::vector<std::string> &keys,
                          int tid)
{
    for (size_t index = 0; index < expected.size(); ++index) {
        if (std::memcmp(addresses[index], expected[index].data(), expected[index].size()) != 0) {
            Log(tid, "[VERIFY_FAIL] phase=D2H key=", keys[index], " data mismatch");
            return false;
        }
    }
    return true;
}

bool VerifyLoadedBuffers(GpuResources &resources, const std::vector<std::vector<uint8_t>> &expected,
                         const std::vector<std::string> &keys, int tid)
{
    std::vector<std::vector<uint8_t>> actual;
    actual.reserve(expected.size());
    for (size_t index = 0; index < expected.size(); ++index) {
        actual.emplace_back(expected[index].size());
        if (!CheckCuda(cudaMemcpyAsync(actual.back().data(), resources.destinationBuffers[index]->Data(),
                                       actual.back().size(), cudaMemcpyDeviceToHost,
                                       reinterpret_cast<cudaStream_t>(resources.stream->Data())),
                       tid, "verification D2H")) {
            return false;
        }
    }
    if (!CheckCuda(resources.stream->Synchronize(), tid, "verification D2H synchronize")) {
        return false;
    }
    for (size_t index = 0; index < expected.size(); ++index) {
        if (actual[index] != expected[index]) {
            Log(tid, "[VERIFY_FAIL] phase=H2D key=", keys[index], " data mismatch");
            return false;
        }
    }
    return true;
}

void PrintShellHelp()
{
    std::cout << "Commands:\n"
              << "  create_set <key> [size]   Create, D2H and Set a Buffer\n"
              << "  get <key> [size]          Get a Buffer and complete Buffer-to-GPU H2D\n"
              << "  roundtrip <key> [size]    Create, D2H, Set, Get and H2D\n"
              << "  mcreate_set <prefix> <count> [size]  MCreate, batched D2H and MSet\n"
              << "  mget <prefix> <count> [size]  Batch Get and complete batched H2D\n"
              << "  mroundtrip <prefix> <count> [size]  MCreate, D2H, MSet, MGet and H2D\n"
              << "  parallel <op> <prefix> <request_count> <threads> [size] [batch_size]\n"
              << "  qps <op> <prefix> <qps> <time_seconds> <threads> [size] [batch_size]\n"
              << "                             op: create_set/mcreate_set/get/mget/roundtrip/mroundtrip\n"
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

bool CreateAndUnloadOne(KVClient &client, const Options &options, const std::string &key, uint64_t size, int tid,
                        GpuResources &resources, PendingBufferMap *pendingBuffers, Summary &summary,
                        LatencySamples *samples = nullptr, bool quiet = false)
{
    if (pendingBuffers != nullptr && !options.deleteAfter) {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        if (pendingBuffers->count(key) != 0) {
            Log(tid, "[CREATE_FAIL] key=", key, " already has a Buffer waiting for Set");
            return false;
        }
    }
    if (options.cleanupBefore) {
        DeleteKey(client, key, tid);
    }
    std::vector<std::vector<uint8_t>> expected;
    if (!PrepareGpuSources(resources, { key }, size, options.verify, tid, expected)) {
        return false;
    }
    std::shared_ptr<datasystem::Buffer> buffer;
    SetParam setParam;
    setParam.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
    Status rc;
    const int64_t createUs = MeasureUs([&] { rc = client.Create(key, size, setParam, buffer); });
    if (!quiet) {
        Log(tid, "[CREATE] key=", key, " size=", size, " rc=", rc.ToString(), " elapsed_us=", createUs);
    }
    if (rc.IsError() || buffer == nullptr || buffer->MutableData() == nullptr) {
        AddStageFailure(samples, "create");
        Log(tid, "[CREATE_FAIL] key=", key, " rc=", rc.ToString(), " buffer=", buffer.get());
        return false;
    }
    AddLatency(samples, "create", createUs);
    ++summary.createOk;
    std::vector<void *> addresses{ buffer->MutableData() };
    TransferTiming timing;
    if (!RunDsTransfer(client, resources, addresses, { size }, true, tid, "create", samples, quiet, timing)) {
        return false;
    }
    ++summary.d2hOk;
    if (options.verify && !VerifyCreatedBuffers(addresses, expected, { key }, tid)) {
        AddStageFailure(samples, "verify");
        return false;
    }
    if (options.verify) {
        ++summary.verifyOk;
    }
    if (pendingBuffers != nullptr && !options.deleteAfter) {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        pendingBuffers->emplace(key, PendingBuffer{ std::move(buffer), size });
    }
    return true;
}

bool SetOnePending(KVClient &client, const Options &options, const std::string &key, int tid,
                   PendingBufferMap &pendingBuffers, Summary &summary, LatencySamples *samples = nullptr,
                   bool quiet = false)
{
    std::shared_ptr<datasystem::Buffer> buffer;
    {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        auto entry = pendingBuffers.find(key);
        if (entry == pendingBuffers.end()) {
            AddStageFailure(samples, "set");
            Log(tid, "[SET_FAIL] key=", key, " has no pending Create Buffer");
            return false;
        }
        buffer = entry->second.buffer;
    }
    Status rc;
    const int64_t setUs = MeasureUs([&] { rc = client.Set(buffer); });
    if (!quiet) {
        Log(tid, "[SET] key=", key, " rc=", rc.ToString(), " elapsed_us=", setUs);
    }
    if (rc.IsError()) {
        AddStageFailure(samples, "set");
        Log(tid, "[SET_FAIL] key=", key, " rc=", rc.ToString());
        return false;
    }
    AddLatency(samples, "set", setUs);
    ++summary.setOk;
    {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        pendingBuffers.erase(key);
    }
    if (options.deleteAfter) {
        DeleteKey(client, key, tid);
    }
    return true;
}

bool CreateSetOne(KVClient &client, const Options &options, const std::string &key, uint64_t size, int tid,
                  GpuResources &resources, PendingBufferMap &pendingBuffers, Summary &summary,
                  LatencySamples *samples = nullptr, bool quiet = false)
{
    Options commandOptions = options;
    commandOptions.deleteAfter = false;
    if (!CreateAndUnloadOne(client, commandOptions, key, size, tid, resources, &pendingBuffers, summary, samples,
                            quiet)) {
        return false;
    }
    if (!SetOnePending(client, commandOptions, key, tid, pendingBuffers, summary, samples, quiet)) {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        pendingBuffers.erase(key);
        return false;
    }
    if (options.deleteAfter) {
        DeleteKey(client, key, tid);
    }
    return true;
}

bool GetAndLoadOne(KVClient &client, const Options &options, const std::string &key, uint64_t size, int tid,
                   GpuResources &resources, Summary &summary, LatencySamples *samples = nullptr, bool quiet = false)
{
    Optional<ReadOnlyBuffer> buffer;
    Status rc;
    const int64_t getUs = MeasureUs([&] { rc = client.Get(key, buffer, options.timeoutMs); });
    if (!quiet) {
        Log(tid, "[GET] key=", key, " rc=", rc.ToString(), " elapsed_us=", getUs);
    }
    if (rc.IsError() || !buffer || buffer->GetSize() != static_cast<int64_t>(size)) {
        AddStageFailure(samples, "get");
        if (rc.IsError() || !buffer) {
            Log(tid, "[GET_FAIL] key=", key, " rc=", rc.ToString(), " has_buffer=", static_cast<bool>(buffer));
        }
        if (buffer && buffer->GetSize() != static_cast<int64_t>(size)) {
            Log(tid, "[VERIFY_FAIL] key=", key, " expected_size=", size, " actual_size=", buffer->GetSize());
        }
        return false;
    }
    AddLatency(samples, "get", getUs);
    ++summary.getOk;
    std::vector<void *> addresses{ const_cast<void *>(buffer->ImmutableData()) };
    TransferTiming timing;
    if (!RunDsTransfer(client, resources, addresses, { size }, false, tid, "get", samples, quiet, timing)) {
        return false;
    }
    ++summary.h2dOk;
    if (options.verify) {
        std::vector<std::vector<uint8_t>> expected{ MakeExpectedData(key, size) };
        if (!VerifyLoadedBuffers(resources, expected, { key }, tid)) {
            AddStageFailure(samples, "verify");
            return false;
        }
        ++summary.verifyOk;
        if (!quiet) {
            Log(tid, "[VERIFY] key=", key, " PASS");
        }
    }
    if (options.deleteAfter) {
        DeleteKey(client, key, tid);
    }
    return true;
}

bool RoundtripOne(KVClient &client, const Options &options, const std::string &key, uint64_t size, int tid,
                  GpuResources &resources, PendingBufferMap &pendingBuffers, Summary &summary,
                  LatencySamples *samples = nullptr, bool quiet = false)
{
    Options commandOptions = options;
    commandOptions.deleteAfter = false;
    if (!CreateSetOne(client, commandOptions, key, size, tid, resources, pendingBuffers, summary, samples, quiet)) {
        return false;
    }
    if (!GetAndLoadOne(client, commandOptions, key, size, tid, resources, summary, samples, quiet)) {
        return false;
    }
    if (options.deleteAfter) {
        DeleteKey(client, key, tid);
    }
    return true;
}

bool RunShellDataCommand(const std::string &command, std::istringstream &stream, KVClient &client,
                         const Options &options, PendingBufferMap &pendingBuffers, Summary &summary)
{
    std::string key;
    uint64_t size = 0;
    if (!ReadKeyAndSize(stream, options, key, size)) {
        Log(0, "[COMMAND_ERROR] usage: ", command, " <key> [size]");
        return false;
    }
    GpuResources resources;
    if (!InitializeGpuResources(resources, options, 1, size, 0)) {
        return false;
    }
    if (command == "create_set") {
        return CreateSetOne(client, options, key, size, 0, resources, pendingBuffers, summary);
    }
    if (command == "get") {
        return GetAndLoadOne(client, options, key, size, 0, resources, summary);
    }
    return RoundtripOne(client, options, key, size, 0, resources, pendingBuffers, summary);
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

bool MCreateAndUnload(KVClient &client, const Options &options, const std::string &batchPrefix, int batchSize,
                      uint64_t size, int tid, GpuResources &resources, PendingBufferMap *pendingBuffers,
                      PendingBatchMap *pendingBatches, Summary &summary, LatencySamples *samples = nullptr,
                      bool quiet = false)
{
    auto keys = BuildBatchKeys(batchPrefix, batchSize);
    if (pendingBuffers != nullptr && pendingBatches != nullptr && !options.deleteAfter) {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        if (pendingBatches->count(batchPrefix) != 0) {
            Log(tid, "[MCREATE_FAIL] prefix=", batchPrefix, " already has a pending batch");
            return false;
        }
        for (const auto &key : keys) {
            if (pendingBuffers->count(key) != 0) {
                Log(tid, "[MCREATE_FAIL] key=", key, " already has a pending Buffer");
                return false;
            }
        }
    }
    if (options.cleanupBefore) {
        for (const auto &key : keys) {
            DeleteKey(client, key, tid);
        }
    }
    std::vector<std::vector<uint8_t>> expected;
    if (!PrepareGpuSources(resources, keys, size, options.verify, tid, expected)) {
        return false;
    }
    std::vector<std::shared_ptr<datasystem::Buffer>> buffers;
    std::vector<uint64_t> sizes(keys.size(), size);
    SetParam setParam;
    setParam.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
    Status rc;
    const int64_t createUs = MeasureUs([&] { rc = client.MCreate(keys, sizes, setParam, buffers); });
    if (!quiet) {
        Log(tid, "[MCREATE] prefix=", batchPrefix, " count=", batchSize, " rc=", rc.ToString(),
            " elapsed_us=", createUs);
    }
    if (rc.IsError() || buffers.size() != keys.size()) {
        AddStageFailure(samples, "create");
        Log(tid, "[MCREATE_FAIL] prefix=", batchPrefix, " rc=", rc.ToString(), " expected_buffers=",
            keys.size(), " actual_buffers=", buffers.size());
        return false;
    }
    std::vector<void *> addresses;
    addresses.reserve(buffers.size());
    for (const auto &buffer : buffers) {
        if (buffer == nullptr || buffer->MutableData() == nullptr || buffer->GetSize() != static_cast<int64_t>(size)) {
            AddStageFailure(samples, "create");
            Log(tid, "[MCREATE_FAIL] invalid returned Buffer");
            return false;
        }
        addresses.emplace_back(buffer->MutableData());
    }
    AddLatency(samples, "create", createUs);
    summary.createOk += batchSize;
    TransferTiming timing;
    if (!RunDsTransfer(client, resources, addresses, sizes, true, tid, "mcreate", samples, quiet, timing)) {
        return false;
    }
    summary.d2hOk += batchSize;
    if (options.verify && !VerifyCreatedBuffers(addresses, expected, keys, tid)) {
        AddStageFailure(samples, "verify");
        return false;
    }
    if (options.verify) {
        summary.verifyOk += batchSize;
        if (!quiet) {
            Log(tid, "[VERIFY] phase=D2H prefix=", batchPrefix, " count=", batchSize, " PASS");
        }
    }
    if (pendingBuffers != nullptr && pendingBatches != nullptr && !options.deleteAfter) {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        for (size_t index = 0; index < keys.size(); ++index) {
            pendingBuffers->emplace(keys[index], PendingBuffer{ std::move(buffers[index]), size });
        }
        pendingBatches->emplace(batchPrefix, std::move(keys));
    }
    return true;
}

bool MSetPendingV2(KVClient &client, const Options &options, const std::string &batchPrefix, int tid,
                   PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches, Summary &summary,
                   LatencySamples *samples = nullptr, bool quiet = false)
{
    std::vector<std::string> keys;
    std::vector<std::shared_ptr<datasystem::Buffer>> buffers;
    {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        auto batch = pendingBatches.find(batchPrefix);
        if (batch == pendingBatches.end()) {
            AddStageFailure(samples, "set");
            Log(tid, "[MSET_FAIL] prefix=", batchPrefix, " has no pending MCreate batch");
            return false;
        }
        keys = batch->second;
        buffers.reserve(keys.size());
        for (const auto &key : keys) {
            auto entry = pendingBuffers.find(key);
            if (entry == pendingBuffers.end()) {
                AddStageFailure(samples, "set");
                Log(tid, "[MSET_FAIL] missing pending Buffer, key=", key);
                return false;
            }
            buffers.emplace_back(entry->second.buffer);
        }
    }
    Status rc;
    const int64_t setUs = MeasureUs([&] { rc = client.MSet(buffers); });
    if (!quiet) {
        Log(tid, "[MSET] prefix=", batchPrefix, " count=", buffers.size(), " rc=", rc.ToString(),
            " elapsed_us=", setUs);
    }
    if (rc.IsError()) {
        AddStageFailure(samples, "set");
        Log(tid, "[MSET_FAIL] prefix=", batchPrefix, " rc=", rc.ToString());
        return false;
    }
    AddLatency(samples, "set", setUs);
    summary.setOk += buffers.size();
    {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        for (const auto &key : keys) {
            pendingBuffers.erase(key);
        }
        pendingBatches.erase(batchPrefix);
    }
    if (options.deleteAfter) {
        for (const auto &key : keys) {
            DeleteKey(client, key, tid);
        }
    }
    return true;
}

bool MCreateSet(KVClient &client, const Options &options, const std::string &batchPrefix, int batchSize,
                uint64_t size, int tid, GpuResources &resources, PendingBufferMap &pendingBuffers,
                PendingBatchMap &pendingBatches, Summary &summary, LatencySamples *samples = nullptr,
                bool quiet = false)
{
    Options commandOptions = options;
    commandOptions.deleteAfter = false;
    if (!MCreateAndUnload(client, commandOptions, batchPrefix, batchSize, size, tid, resources, &pendingBuffers,
                          &pendingBatches, summary, samples, quiet)) {
        return false;
    }
    if (!MSetPendingV2(client, commandOptions, batchPrefix, tid, pendingBuffers, pendingBatches, summary, samples,
                       quiet)) {
        std::lock_guard<std::mutex> lock(g_pendingMutex);
        auto batch = pendingBatches.find(batchPrefix);
        if (batch != pendingBatches.end()) {
            for (const auto &key : batch->second) {
                pendingBuffers.erase(key);
            }
            pendingBatches.erase(batch);
        }
        return false;
    }
    if (options.deleteAfter) {
        for (const auto &key : BuildBatchKeys(batchPrefix, batchSize)) {
            DeleteKey(client, key, tid);
        }
    }
    return true;
}

bool MGetAndLoad(KVClient &client, const Options &options, const std::string &batchPrefix, int batchSize,
                 uint64_t size, int tid, GpuResources &resources, Summary &summary,
                 LatencySamples *samples = nullptr, bool quiet = false)
{
    const auto keys = BuildBatchKeys(batchPrefix, batchSize);
    std::vector<Optional<ReadOnlyBuffer>> buffers;
    Status rc;
    const int64_t getUs = MeasureUs([&] { rc = client.Get(keys, buffers, options.timeoutMs); });
    if (!quiet) {
        Log(tid, "[MGET] prefix=", batchPrefix, " count=", batchSize, " rc=", rc.ToString(),
            " elapsed_us=", getUs);
    }
    if (rc.IsError() || buffers.size() != keys.size()) {
        AddStageFailure(samples, "get");
        Log(tid, "[MGET_FAIL] prefix=", batchPrefix, " rc=", rc.ToString(), " expected_buffers=", keys.size(),
            " actual_buffers=", buffers.size());
        return false;
    }
    std::vector<void *> addresses;
    std::vector<uint64_t> sizes(keys.size(), size);
    addresses.reserve(keys.size());
    size_t missingCount = 0;
    size_t nullDataCount = 0;
    size_t sizeMismatchCount = 0;
    for (size_t index = 0; index < keys.size(); ++index) {
        if (!buffers[index]) {
            ++missingCount;
            Log(tid, "[MGET_BUFFER_FAIL] key=", keys[index], " index=", index,
                " reason=missing batch_rc=", rc.ToString());
            continue;
        }
        if (buffers[index]->ImmutableData() == nullptr) {
            ++nullDataCount;
            Log(tid, "[MGET_BUFFER_FAIL] key=", keys[index], " index=", index,
                " reason=null_data actual_size=", buffers[index]->GetSize());
            continue;
        }
        if (buffers[index]->GetSize() != static_cast<int64_t>(size)) {
            ++sizeMismatchCount;
            Log(tid, "[MGET_BUFFER_FAIL] key=", keys[index], " index=", index,
                " reason=size_mismatch expected_size=", size, " actual_size=", buffers[index]->GetSize());
            continue;
        }
        addresses.emplace_back(const_cast<void *>(buffers[index]->ImmutableData()));
    }
    const size_t invalidCount = missingCount + nullDataCount + sizeMismatchCount;
    if (invalidCount != 0) {
        AddStageFailure(samples, "get");
        Log(tid, "[MGET_FAIL] prefix=", batchPrefix, " count=", batchSize, " invalid_buffers=", invalidCount,
            " missing=", missingCount, " null_data=", nullDataCount,
            " size_mismatch=", sizeMismatchCount, " batch_rc=", rc.ToString());
        return false;
    }
    AddLatency(samples, "get", getUs);
    summary.getOk += batchSize;
    TransferTiming timing;
    if (!RunDsTransfer(client, resources, addresses, sizes, false, tid, "mget", samples, quiet, timing)) {
        return false;
    }
    summary.h2dOk += batchSize;
    if (options.verify) {
        std::vector<std::vector<uint8_t>> expected;
        expected.reserve(keys.size());
        for (const auto &key : keys) {
            expected.emplace_back(MakeExpectedData(key, size));
        }
        if (!VerifyLoadedBuffers(resources, expected, keys, tid)) {
            AddStageFailure(samples, "verify");
            return false;
        }
        summary.verifyOk += batchSize;
        if (!quiet) {
            Log(tid, "[VERIFY] phase=H2D prefix=", batchPrefix, " count=", batchSize, " PASS");
        }
    }
    if (options.deleteAfter) {
        for (const auto &key : keys) {
            DeleteKey(client, key, tid);
        }
    }
    return true;
}

bool MRoundtrip(KVClient &client, const Options &options, const std::string &batchPrefix, int batchSize,
                uint64_t size, int tid, GpuResources &resources, PendingBufferMap &pendingBuffers,
                PendingBatchMap &pendingBatches, Summary &summary, LatencySamples *samples = nullptr,
                bool quiet = false)
{
    Options commandOptions = options;
    commandOptions.deleteAfter = false;
    if (!MCreateSet(client, commandOptions, batchPrefix, batchSize, size, tid, resources, pendingBuffers,
                    pendingBatches, summary, samples, quiet)) {
        return false;
    }
    if (!MGetAndLoad(client, commandOptions, batchPrefix, batchSize, size, tid, resources, summary, samples,
                     quiet)) {
        return false;
    }
    if (options.deleteAfter) {
        for (const auto &key : BuildBatchKeys(batchPrefix, batchSize)) {
            DeleteKey(client, key, tid);
        }
    }
    return true;
}

struct WorkloadSpec {
    std::string operation;
    std::string prefix;
    int64_t requestCount = 0;
    int threadNum = 0;
    uint64_t size = 0;
    int batchSize = 1;
    int qps = 0;
    int timeSeconds = 0;
    int legacyKeysPerThread = 0;
};

struct WorkloadCounters {
    std::atomic<int64_t> requestOk{ 0 };
    std::atomic<int64_t> requestFailed{ 0 };
};

bool IsSupportedWorkloadOperation(const std::string &operation)
{
    return operation == "create_set" || operation == "mcreate_set" || operation == "get"
           || operation == "mget" || operation == "roundtrip" || operation == "mroundtrip";
}

bool IsBatchWorkloadOperation(const std::string &operation)
{
    return !operation.empty() && operation.front() == 'm';
}

bool OperationNeedsGpu(const std::string &operation)
{
    return IsSupportedWorkloadOperation(operation);
}

std::string RequestPrefix(const WorkloadSpec &spec, int64_t requestIndex)
{
    if (spec.legacyKeysPerThread > 0) {
        return spec.prefix + "_T" + std::to_string(requestIndex / spec.legacyKeysPerThread) + "_"
               + std::to_string(requestIndex % spec.legacyKeysPerThread);
    }
    return spec.prefix + "_" + std::to_string(requestIndex);
}

int64_t PercentileNearestRank(const std::vector<int64_t> &sorted, double percentile)
{
    if (sorted.empty()) {
        return 0;
    }
    const size_t rank = static_cast<size_t>(std::ceil(percentile * static_cast<double>(sorted.size())));
    return sorted[std::min(std::max<size_t>(rank, 1), sorted.size()) - 1];
}

void PrintLatencyReport(LatencySamples &samples, const WorkloadSpec &spec, const WorkloadCounters &counters)
{
    std::lock_guard<std::mutex> sampleLock(samples.mutex);
    std::lock_guard<std::mutex> logLock(g_logMutex);
    std::cout << "========== WORKLOAD LATENCY ==========" << '\n'
              << "operation          : " << spec.operation << '\n'
              << "requests success   : " << counters.requestOk.load() << '\n'
              << "requests failed    : " << counters.requestFailed.load() << '\n';
    const std::vector<std::string> stages{ "create", "d2h", "set", "get", "h2d", "queue_wait",
                                           "request_total" };
    std::cout << std::left << std::setw(16) << "stage" << std::right << std::setw(12) << "success"
              << std::setw(12) << "failed"
              << std::setw(14) << "avg_us" << std::setw(12) << "p95_us" << std::setw(12) << "p99_us"
              << std::setw(14) << "p99.99_us" << std::setw(12) << "pmax_us" << '\n';
    for (const auto &stage : stages) {
        auto found = samples.values.find(stage);
        const int64_t failed = samples.failures.count(stage) == 0 ? 0 : samples.failures.at(stage);
        if ((found == samples.values.end() || found->second.empty()) && failed == 0) {
            continue;
        }
        auto values = found == samples.values.end() ? std::vector<int64_t>{} : found->second;
        std::sort(values.begin(), values.end());
        long double sum = 0;
        for (const auto value : values) {
            sum += value;
        }
        const long double average = values.empty() ? 0 : sum / static_cast<long double>(values.size());
        std::cout << std::left << std::setw(16) << stage << std::right << std::setw(12) << values.size()
                  << std::setw(12) << failed
                  << std::setw(14) << std::fixed << std::setprecision(2) << static_cast<double>(average)
                  << std::setw(12) << PercentileNearestRank(values, 0.95)
                  << std::setw(12) << PercentileNearestRank(values, 0.99)
                  << std::setw(14) << PercentileNearestRank(values, 0.9999)
                  << std::setw(12) << (values.empty() ? 0 : values.back())
                  << '\n';
    }
    std::cout << std::defaultfloat;
}

bool PrepareWorkload(const std::shared_ptr<KVClient> &client, const Options &options, const WorkloadSpec &spec,
                     GpuResources &resources, PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches)
{
    const bool prepareGet = spec.operation == "get" || spec.operation == "mget";
    if (!prepareGet) {
        return true;
    }
    const int objectCount = IsBatchWorkloadOperation(spec.operation) ? spec.batchSize : 1;
    Options prepareOptions = options;
    prepareOptions.deleteAfter = false;
    Summary prepareSummary;
    Log(0, "[PREPARE] begin operation=", spec.operation, " requests=", spec.requestCount,
        " objects=", spec.requestCount * objectCount, " bytes=", spec.requestCount * objectCount * spec.size);
    for (int64_t index = 0; index < spec.requestCount; ++index) {
        const std::string requestPrefix = RequestPrefix(spec, index);
        bool success = true;
        if (spec.operation == "get") {
            success = CreateSetOne(*client, prepareOptions, requestPrefix, spec.size, 0, resources, pendingBuffers,
                                   prepareSummary, nullptr, true);
        } else {
            success = MCreateSet(*client, prepareOptions, requestPrefix, spec.batchSize, spec.size, 0, resources,
                                 pendingBuffers, pendingBatches, prepareSummary, nullptr, true);
        }
        if (!success) {
            Log(0, "[PREPARE_FAIL] operation=", spec.operation, " request_index=", index);
            return false;
        }
    }
    Log(0, "[PREPARE] end operation=", spec.operation, " result=OK");
    return true;
}

bool ExecuteWorkloadRequest(const std::shared_ptr<KVClient> &client, const Options &options,
                            const WorkloadSpec &spec, int64_t requestIndex, int tid, GpuResources *resources,
                            bool quiet, PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches,
                            Summary &summary, LatencySamples &samples, WorkloadCounters &counters)
{
    const auto begin = std::chrono::steady_clock::now();
    const std::string requestPrefix = RequestPrefix(spec, requestIndex);
    bool success = false;
    if (spec.operation == "create_set") {
        success = CreateSetOne(*client, options, requestPrefix, spec.size, tid, *resources, pendingBuffers, summary,
                               &samples, quiet);
    } else if (spec.operation == "mcreate_set") {
        success = MCreateSet(*client, options, requestPrefix, spec.batchSize, spec.size, tid, *resources,
                             pendingBuffers, pendingBatches, summary, &samples, quiet);
    } else if (spec.operation == "get") {
        success = GetAndLoadOne(*client, options, requestPrefix, spec.size, tid, *resources, summary, &samples,
                                quiet);
    } else if (spec.operation == "mget") {
        success = MGetAndLoad(*client, options, requestPrefix, spec.batchSize, spec.size, tid, *resources, summary,
                              &samples, quiet);
    } else if (spec.operation == "roundtrip") {
        success = RoundtripOne(*client, options, requestPrefix, spec.size, tid, *resources, pendingBuffers,
                               summary, &samples, quiet);
    } else {
        success = MRoundtrip(*client, options, requestPrefix, spec.batchSize, spec.size, tid, *resources,
                             pendingBuffers, pendingBatches, summary, &samples, quiet);
    }
    const int64_t totalUs = std::chrono::duration_cast<std::chrono::microseconds>(
                                std::chrono::steady_clock::now() - begin)
                                .count();
    if (success) {
        samples.Add("request_total", totalUs);
        ++counters.requestOk;
    } else {
        samples.Fail("request_total");
        ++counters.requestFailed;
        ++summary.failed;
        Log(tid, "[RESULT] operation=", spec.operation, " request_index=", requestIndex, " key_prefix=",
            requestPrefix, " FAIL");
    }
    return success;
}

bool ParseParallelSpec(std::istringstream &stream, const Options &options, WorkloadSpec &spec)
{
    if (!(stream >> spec.operation >> spec.prefix >> spec.requestCount >> spec.threadNum)
        || spec.requestCount <= 0 || spec.threadNum <= 0 || !IsSupportedWorkloadOperation(spec.operation)
        || !ParseCommandSize(stream, options.valueSize, spec.size)) {
        return false;
    }
    spec.batchSize = IsBatchWorkloadOperation(spec.operation) ? options.count : 1;
    if (IsBatchWorkloadOperation(spec.operation)) {
        std::string batchSize;
        if (stream >> batchSize) {
            spec.batchSize = std::stoi(batchSize);
        }
    }
    return spec.batchSize > 0;
}

bool RunParallelWorkload(const std::shared_ptr<KVClient> &client, const Options &options, const WorkloadSpec &spec,
                         PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches, Summary &summary)
{
    std::vector<GpuResources> resourcePool;
    if (!InitializeGpuResourcePool(resourcePool, options, spec.threadNum, static_cast<size_t>(spec.batchSize),
                                   spec.size)) {
        Log(0, "[PARALLEL_FAIL] shared CUDA resource initialization failed");
        return false;
    }
    if (!PrepareWorkload(client, options, spec, resourcePool.front(), pendingBuffers, pendingBatches)) {
        return false;
    }
    LatencySamples samples;
    WorkloadCounters counters;
    std::atomic<int64_t> next{ 0 };
    std::mutex readyMutex;
    std::condition_variable readyCv;
    int readyCount = 0;
    bool initializeFailed = false;
    bool startWorkers = false;
    std::vector<std::thread> threads;
    for (int tid = 0; tid < spec.threadNum; ++tid) {
        threads.emplace_back([&, tid] {
            // cudaSetDevice selects the process-wide primary context for this host thread. Streams and
            // device buffers were already created once by the main thread before workers were started.
            const bool initialized = CheckCuda(cudaSetDevice(options.gpuId), tid, "cudaSetDevice worker");
            bool abortWorker = false;
            {
                std::unique_lock<std::mutex> lock(readyMutex);
                initializeFailed = initializeFailed || !initialized;
                ++readyCount;
                // The main thread and initialized workers wait on the same condition variable.
                // Wake all waiters so the notification cannot be consumed only by another worker,
                // leaving the main thread asleep after readyCount reaches threadNum.
                readyCv.notify_all();
                readyCv.wait(lock, [&] { return startWorkers || initializeFailed; });
                abortWorker = initializeFailed;
            }
            if (!initialized || abortWorker) {
                return;
            }
            for (int64_t index = next.fetch_add(1); index < spec.requestCount; index = next.fetch_add(1)) {
                ExecuteWorkloadRequest(client, options, spec, index, tid,
                                       OperationNeedsGpu(spec.operation)
                                           ? &resourcePool[static_cast<size_t>(tid)]
                                           : nullptr,
                                       false, pendingBuffers, pendingBatches, summary, samples, counters);
            }
        });
    }
    {
        std::unique_lock<std::mutex> lock(readyMutex);
        readyCv.wait(lock, [&] { return readyCount == spec.threadNum || initializeFailed; });
        startWorkers = !initializeFailed;
    }
    readyCv.notify_all();
    for (auto &thread : threads) {
        thread.join();
    }
    if (initializeFailed) {
        Log(0, "[PARALLEL_FAIL] worker GPU resource initialization failed");
        return false;
    }
    PrintLatencyReport(samples, spec, counters);
    return true;
}

bool RunParallel(std::istringstream &stream, const std::shared_ptr<KVClient> &client, const Options &options,
                 PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches, Summary &summary)
{
    WorkloadSpec spec;
    if (!ParseParallelSpec(stream, options, spec)) {
        Log(0, "[COMMAND_ERROR] usage: parallel <create_set|mcreate_set|get|mget|roundtrip|mroundtrip> "
               "<prefix> <request_count> <threads> [size] [batch_size]");
        return false;
    }
    return RunParallelWorkload(client, options, spec, pendingBuffers, pendingBatches, summary);
}

struct QpsTask {
    int64_t requestIndex = 0;
    std::chrono::steady_clock::time_point enqueuedAt;
};

bool ParseQpsSpec(std::istringstream &stream, const Options &options, WorkloadSpec &spec)
{
    if (!(stream >> spec.operation >> spec.prefix >> spec.qps >> spec.timeSeconds >> spec.threadNum)
        || spec.qps <= 0 || spec.timeSeconds <= 0 || spec.threadNum <= 0
        || !IsSupportedWorkloadOperation(spec.operation) || !ParseCommandSize(stream, options.valueSize, spec.size)) {
        return false;
    }
    spec.batchSize = IsBatchWorkloadOperation(spec.operation) ? options.count : 1;
    if (IsBatchWorkloadOperation(spec.operation)) {
        std::string batchSize;
        if (stream >> batchSize) {
            spec.batchSize = std::stoi(batchSize);
        }
    }
    if (spec.batchSize <= 0
        || static_cast<int64_t>(spec.qps) > std::numeric_limits<int64_t>::max() / spec.timeSeconds) {
        return false;
    }
    spec.requestCount = static_cast<int64_t>(spec.qps) * spec.timeSeconds;
    return true;
}

bool RunQps(std::istringstream &stream, const std::shared_ptr<KVClient> &client, const Options &options,
            PendingBufferMap &pendingBuffers, PendingBatchMap &pendingBatches, Summary &summary)
{
    WorkloadSpec spec;
    if (!ParseQpsSpec(stream, options, spec)) {
        Log(0, "[COMMAND_ERROR] usage: qps <create_set|mcreate_set|get|mget|roundtrip|mroundtrip> "
               "<prefix> <qps> <time_seconds> <threads> [size] [batch_size]");
        return false;
    }
    std::vector<GpuResources> resourcePool;
    if (!InitializeGpuResourcePool(resourcePool, options, spec.threadNum, static_cast<size_t>(spec.batchSize),
                                   spec.size)) {
        Log(0, "[QPS_FAIL] shared CUDA resource initialization failed");
        return false;
    }
    if (!PrepareWorkload(client, options, spec, resourcePool.front(), pendingBuffers, pendingBatches)) {
        return false;
    }

    std::mutex queueMutex;
    std::condition_variable queueCv;
    std::deque<QpsTask> queue;
    bool producerDone = false;
    std::mutex readyMutex;
    std::condition_variable readyCv;
    int readyCount = 0;
    bool initializeFailed = false;
    LatencySamples samples;
    WorkloadCounters counters;
    std::vector<std::thread> threads;
    for (int tid = 0; tid < spec.threadNum; ++tid) {
        threads.emplace_back([&, tid] {
            // Use the shared stream/context selected during main-thread initialization. Each worker
            // still owns a distinct source/destination buffer set in resourcePool[tid].
            const bool initialized = CheckCuda(cudaSetDevice(options.gpuId), tid, "cudaSetDevice worker");
            {
                std::lock_guard<std::mutex> lock(readyMutex);
                initializeFailed = initializeFailed || !initialized;
                ++readyCount;
            }
            readyCv.notify_one();
            if (!initialized) {
                return;
            }
            while (true) {
                QpsTask task;
                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    queueCv.wait(lock, [&] { return producerDone || !queue.empty(); });
                    if (queue.empty()) {
                        if (producerDone) {
                            break;
                        }
                        continue;
                    }
                    task = queue.front();
                    queue.pop_front();
                }
                samples.Add("queue_wait", std::chrono::duration_cast<std::chrono::microseconds>(
                                                   std::chrono::steady_clock::now() - task.enqueuedAt)
                                                   .count());
                ExecuteWorkloadRequest(client, options, spec, task.requestIndex, tid,
                                       OperationNeedsGpu(spec.operation)
                                           ? &resourcePool[static_cast<size_t>(tid)]
                                           : nullptr,
                                       true, pendingBuffers, pendingBatches, summary, samples, counters);
            }
        });
    }
    {
        std::unique_lock<std::mutex> lock(readyMutex);
        readyCv.wait(lock, [&] { return readyCount == spec.threadNum; });
    }
    if (initializeFailed) {
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            producerDone = true;
        }
        queueCv.notify_all();
        for (auto &thread : threads) {
            thread.join();
        }
        Log(0, "[QPS_FAIL] worker GPU resource initialization failed");
        return false;
    }

    Log(0, "[QPS] begin operation=", spec.operation, " target_qps=", spec.qps,
        " time_seconds=", spec.timeSeconds, " threads=", spec.threadNum, " requests=", spec.requestCount,
        " batch_size=", spec.batchSize, " value_size=", spec.size);
    const auto workloadBegin = std::chrono::steady_clock::now();
    std::atomic<int64_t> submittedCount{ 0 };
    std::mutex progressMutex;
    std::condition_variable progressCv;
    bool progressDone = false;
    std::thread progressThread([&] {
        auto nextReport = workloadBegin + std::chrono::seconds(1);
        std::unique_lock<std::mutex> progressLock(progressMutex);
        while (!progressCv.wait_until(progressLock, nextReport, [&] { return progressDone; })) {
            progressLock.unlock();

            size_t queued = 0;
            bool submitDone = false;
            {
                std::lock_guard<std::mutex> queueLock(queueMutex);
                queued = queue.size();
                submitDone = producerDone;
            }
            const auto now = std::chrono::steady_clock::now();
            const int64_t elapsedMs =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - workloadBegin).count();
            const int64_t submitted = submittedCount.load(std::memory_order_relaxed);
            const int64_t succeeded = counters.requestOk.load(std::memory_order_relaxed);
            const int64_t failed = counters.requestFailed.load(std::memory_order_relaxed);
            const int64_t completed = succeeded + failed;
            const int64_t inFlight =
                std::max<int64_t>(0, submitted - completed - static_cast<int64_t>(queued));
            const int64_t completeQps = elapsedMs > 0 ? completed * 1000 / elapsedMs : 0;
            Log(0, "[QPS_PROGRESS] phase=", submitDone ? "draining" : "submitting",
                " elapsed_ms=", elapsedMs, " submitted=", submitted, "/", spec.requestCount,
                " completed=", completed, " success=", succeeded, " failed=", failed,
                " queued=", queued, " in_flight=", inFlight, " avg_complete_qps=", completeQps);

            progressLock.lock();
            nextReport += std::chrono::seconds(1);
            if (nextReport <= std::chrono::steady_clock::now()) {
                nextReport = std::chrono::steady_clock::now() + std::chrono::seconds(1);
            }
        }
    });
    for (int64_t index = 0; index < spec.requestCount; ++index) {
        const auto target = workloadBegin
                            + std::chrono::nanoseconds(static_cast<int64_t>(
                                (static_cast<long double>(index) * 1000000000.0L) / spec.qps));
        std::this_thread::sleep_until(target);
        {
            std::lock_guard<std::mutex> lock(queueMutex);
            queue.emplace_back(QpsTask{ index, std::chrono::steady_clock::now() });
            submittedCount.fetch_add(1, std::memory_order_relaxed);
        }
        queueCv.notify_one();
    }
    std::this_thread::sleep_until(workloadBegin + std::chrono::seconds(spec.timeSeconds));
    const auto submitEnd = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        producerDone = true;
    }
    queueCv.notify_all();
    for (auto &thread : threads) {
        thread.join();
    }
    const auto workloadEnd = std::chrono::steady_clock::now();
    {
        std::lock_guard<std::mutex> lock(progressMutex);
        progressDone = true;
    }
    progressCv.notify_one();
    progressThread.join();
    const double submitSeconds = std::chrono::duration<double>(submitEnd - workloadBegin).count();
    const double drainSeconds = std::chrono::duration<double>(workloadEnd - submitEnd).count();
    const double totalSeconds = std::chrono::duration<double>(workloadEnd - workloadBegin).count();
    {
        std::lock_guard<std::mutex> lock(g_logMutex);
        std::cout << "========== QPS SUMMARY ==========\n"
                  << "target qps         : " << spec.qps << '\n'
                  << "target time s      : " << spec.timeSeconds << '\n'
                  << "submitted requests : " << spec.requestCount << '\n'
                  << "submit time s      : " << std::fixed << std::setprecision(6) << submitSeconds << '\n'
                  << "drain time s       : " << drainSeconds << '\n'
                  << "total time s       : " << totalSeconds << '\n'
                  << "actual submit qps  : " << (submitSeconds > 0 ? spec.requestCount / submitSeconds : 0) << '\n'
                  << "actual complete qps: "
                  << (totalSeconds > 0
                          ? (counters.requestOk.load() + counters.requestFailed.load()) / totalSeconds
                          : 0)
                  << '\n'
                  << std::defaultfloat;
    }
    PrintLatencyReport(samples, spec, counters);
    return true;
}

bool RunShellBatchCommand(const std::string &command, std::istringstream &stream, KVClient &client,
                          const Options &options, PendingBufferMap &pendingBuffers,
                          PendingBatchMap &pendingBatches, Summary &summary)
{
    std::string prefix;
    int count = 0;
    uint64_t size = 0;
    if (!ParseBatchArgs(stream, options, prefix, count, size)) {
        Log(0, "[COMMAND_ERROR] usage: ", command, " <prefix> <count> [size]");
        return false;
    }
    GpuResources resources;
    if (!InitializeGpuResources(resources, options, static_cast<size_t>(count), size, 0)) {
        return false;
    }
    if (command == "mcreate_set") {
        return MCreateSet(client, options, prefix, count, size, 0, resources, pendingBuffers, pendingBatches,
                          summary);
    }
    if (command == "mget") {
        return MGetAndLoad(client, options, prefix, count, size, 0, resources, summary);
    }
    return MRoundtrip(client, options, prefix, count, size, 0, resources, pendingBuffers, pendingBatches, summary);
}

bool IsBatchCommand(const std::string &command)
{
    return command == "mcreate_set" || command == "mget" || command == "mroundtrip";
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
    if (command == "create_set" || command == "get" || command == "roundtrip") {
        return RunShellDataCommand(command, stream, *client, options, pendingBuffers, summary);
    }
    if (IsBatchCommand(command)) {
        return RunShellBatchCommand(command, stream, *client, options, pendingBuffers, pendingBatches, summary);
    }
    if (command == "parallel") {
        return RunParallel(stream, client, options, pendingBuffers, pendingBatches, summary);
    }
    if (command == "qps") {
        return RunQps(stream, client, options, pendingBuffers, pendingBatches, summary);
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
    PendingBufferMap pendingBuffers;
    PendingBatchMap pendingBatches;
    WorkloadSpec spec;
    spec.operation = options.command;
    spec.prefix = options.keyPrefix;
    spec.requestCount = static_cast<int64_t>(options.count) * options.threadNum;
    spec.threadNum = options.threadNum;
    spec.size = options.valueSize;
    spec.legacyKeysPerThread = options.count;
    if (!RunParallelWorkload(client, options, spec, pendingBuffers, pendingBatches, summary)) {
        ++summary.failed;
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

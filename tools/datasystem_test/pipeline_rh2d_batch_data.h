#ifndef PIPELINE_RH2D_BATCH_DATA_H
#define PIPELINE_RH2D_BATCH_DATA_H

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <dirent.h>
#include <fstream>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <unistd.h>

namespace rh2d_batch_data {

using Data = std::vector<std::pair<std::string, std::string>>;

struct SizeConfig {
    size_t size;
    int count;
};

inline std::string GenerateRandomString(size_t length)
{
    static const char chars[] = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    static thread_local std::mt19937 generator(std::random_device{}());
    std::uniform_int_distribution<size_t> distribution(0, sizeof(chars) - 2);
    std::string result(length, '\0');
    for (char &ch : result) {
        ch = chars[distribution(generator)];
    }
    return result;
}

inline std::string BuildSignature(int count, int batch, const std::vector<SizeConfig> &configs)
{
    std::ostringstream stream;
    stream << "count=" << count << ";batch=" << batch << ";sizes=";
    for (const auto &config : configs) {
        stream << config.size << ':' << config.count << ',';
    }
    return stream.str();
}

inline uint64_t HashSignature(const std::string &signature)
{
    uint64_t hash = 1469598103934665603ULL;
    for (unsigned char ch : signature) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    return hash;
}

inline std::string BuildCachePath(const std::string &signature)
{
    std::ostringstream stream;
    stream << "pipeline_rh2d_batch_data_" << std::hex << std::setw(16) << std::setfill('0')
           << HashSignature(signature) << ".bin";
    return stream.str();
}

inline std::string GetConfigSignature(const std::string &signature)
{
    size_t separator = signature.find(';');
    return separator == std::string::npos ? std::string() : signature.substr(separator + 1);
}

inline bool IsCompatibleSignature(const std::string &actual, const std::string &expected)
{
    std::string actualConfig = GetConfigSignature(actual);
    return !actualConfig.empty() && actualConfig == GetConfigSignature(expected);
}

template <typename T>
inline bool WriteNumber(std::ofstream &output, const T &value)
{
    output.write(reinterpret_cast<const char *>(&value), sizeof(value));
    return output.good();
}

template <typename T>
inline bool ReadNumber(std::ifstream &input, T &value)
{
    input.read(reinterpret_cast<char *>(&value), sizeof(value));
    return input.good();
}

inline bool WriteHeader(std::ofstream &output, const std::string &signature, uint64_t count)
{
    static const char magic[8] = {'R', 'H', '2', 'D', 'D', 'A', 'T', 'A'};
    constexpr uint32_t version = 1;
    constexpr uint32_t endian = 0x01020304;
    uint64_t signatureSize = signature.size();
    output.write(magic, sizeof(magic));
    return WriteNumber(output, version) && WriteNumber(output, endian) &&
           WriteNumber(output, count) && WriteNumber(output, signatureSize) &&
           (output.write(signature.data(), signature.size()), output.good());
}

inline bool ReadHeader(std::ifstream &input, const std::string &expectedSignature, uint64_t expectedCount,
                       uint64_t &storedCount, std::string &error)
{
    static const std::string expectedMagic("RH2DDATA", 8);
    char magic[8] = {};
    uint32_t version = 0;
    uint32_t endian = 0;
    uint64_t count = 0;
    uint64_t signatureSize = 0;
    input.read(magic, sizeof(magic));
    if (!ReadNumber(input, version) || !ReadNumber(input, endian) || !ReadNumber(input, count) ||
        !ReadNumber(input, signatureSize) || std::string(magic, sizeof(magic)) != expectedMagic) {
        error = "invalid or incomplete cache header";
        return false;
    }
    if (version != 1 || endian != 0x01020304 || count < expectedCount || signatureSize > 4096) {
        error = "cache metadata does not match this request";
        return false;
    }
    std::string signature(signatureSize, '\0');
    input.read(signature.data(), signature.size());
    if (!input.good() || !IsCompatibleSignature(signature, expectedSignature)) {
        error = "cache parameter signature does not match this request";
        return false;
    }
    storedCount = count;
    return true;
}

inline bool Save(const std::string &path, const std::string &signature, const Data &data, std::string &error)
{
    std::string temporaryPath = path + ".tmp." + std::to_string(static_cast<long long>(getpid()));
    std::ofstream output(temporaryPath, std::ios::binary | std::ios::trunc);
    if (!output || !WriteHeader(output, signature, data.size())) {
        error = "failed to create cache file: " + temporaryPath;
        output.close();
        std::remove(temporaryPath.c_str());
        return false;
    }
    for (const auto &item : data) {
        uint64_t keySize = item.first.size();
        uint64_t valueSize = item.second.size();
        if (!WriteNumber(output, keySize) || !WriteNumber(output, valueSize)) {
            error = "failed to write cache record length";
            break;
        }
        output.write(item.first.data(), item.first.size());
        output.write(item.second.data(), item.second.size());
        if (!output.good()) {
            error = "failed to write cache record data";
            break;
        }
    }
    output.close();
    if (!error.empty() || std::rename(temporaryPath.c_str(), path.c_str()) != 0) {
        if (error.empty()) error = "failed to publish cache file: " + path;
        std::remove(temporaryPath.c_str());
        return false;
    }
    return true;
}

inline bool LoadRecord(std::ifstream &input, size_t maxValueSize, std::pair<std::string, std::string> &item)
{
    uint64_t keySize = 0;
    uint64_t valueSize = 0;
    if (!ReadNumber(input, keySize) || !ReadNumber(input, valueSize) || keySize > 1024 ||
        valueSize > maxValueSize) {
        return false;
    }
    item.first.resize(keySize);
    item.second.resize(valueSize);
    input.read(item.first.data(), item.first.size());
    input.read(item.second.data(), item.second.size());
    return input.good();
}

inline bool Load(const std::string &path, const std::string &signature, size_t expectedCount,
                 size_t maxValueSize, Data &data, std::string &error)
{
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        error = "cache file does not exist";
        return false;
    }
    uint64_t storedCount = 0;
    if (!ReadHeader(input, signature, expectedCount, storedCount, error)) return false;
    Data loaded;
    loaded.reserve(expectedCount);
    for (size_t index = 0; index < expectedCount; ++index) {
        std::pair<std::string, std::string> item;
        if (!LoadRecord(input, maxValueSize, item)) {
            error = "invalid or incomplete cache record at index " + std::to_string(index);
            return false;
        }
        loaded.emplace_back(std::move(item));
    }
    data.swap(loaded);
    return true;
}

inline bool IsCacheFileName(const std::string &name)
{
    static const std::string prefix = "pipeline_rh2d_batch_data_";
    static const std::string suffix = ".bin";
    return name.compare(0, prefix.size(), prefix) == 0 && name.size() > prefix.size() + suffix.size() &&
           name.compare(name.size() - suffix.size(), suffix.size(), suffix) == 0;
}

inline bool LoadCompatible(const std::string &signature, size_t expectedCount, size_t maxValueSize,
                           Data &data, std::string &loadedPath, std::string &error)
{
    DIR *directory = opendir(".");
    if (directory == nullptr) {
        error = "failed to scan current directory";
        return false;
    }
    bool loaded = false;
    while (dirent *entry = readdir(directory)) {
        std::string path = entry->d_name;
        std::string loadError;
        if (!IsCacheFileName(path) || !Load(path, signature, expectedCount, maxValueSize, data, loadError)) {
            continue;
        }
        loadedPath = path;
        loaded = true;
        break;
    }
    closedir(directory);
    if (!loaded) error = "no compatible cache with enough records was found";
    return loaded;
}

template <typename ProgressCallback>
inline void Generate(int count, int batch, const std::vector<SizeConfig> &configs, Data &data,
                     ProgressCallback reportProgress)
{
    data.clear();
    data.reserve(count);
    int numBatches = count / batch;
    for (int batchIndex = 0; batchIndex < numBatches; ++batchIndex) {
        int allocated = 0;
        for (const auto &config : configs) {
            int itemCount = config.count == -1 ? batch - allocated : std::min(config.count, batch - allocated);
            for (int index = 0; index < itemCount; ++index) {
                std::string key = "key_" + std::to_string(data.size()) + "_" + GenerateRandomString(8);
                data.emplace_back(std::move(key), GenerateRandomString(config.size));
                reportProgress(data.size());
            }
            allocated += itemCount;
            if (allocated >= batch) break;
        }
    }
}

}  // namespace rh2d_batch_data

#endif  // PIPELINE_RH2D_BATCH_DATA_H

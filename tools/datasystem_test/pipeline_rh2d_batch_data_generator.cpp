#include "pipeline_rh2d_batch_data.h"

#include <algorithm>
#include <iostream>
#include <stdexcept>

namespace {

struct Args {
    int count = 100;
    int batch = 10;
    std::string valueSizes = "8388608";
    bool help = false;
};

bool SplitArgument(int argc, char *argv[], int &index, std::string &key, std::string &value)
{
    std::string argument = argv[index];
    if (argument.rfind("--", 0) != 0) return false;
    size_t separator = argument.find('=');
    key = argument.substr(2, separator == std::string::npos ? separator : separator - 2);
    if (separator != std::string::npos) {
        value = argument.substr(separator + 1);
    } else if (index + 1 < argc) {
        value = argv[++index];
    } else {
        throw std::invalid_argument("missing value for --" + key);
    }
    return true;
}

Args ParseArgs(int argc, char *argv[])
{
    Args args;
    for (int index = 1; index < argc; ++index) {
        std::string argument = argv[index];
        if (argument == "--help" || argument == "-h") {
            args.help = true;
            continue;
        }
        std::string key;
        std::string value;
        if (!SplitArgument(argc, argv, index, key, value)) {
            throw std::invalid_argument("unknown argument: " + argument);
        }
        if (key == "count") args.count = std::stoi(value);
        else if (key == "batch") args.batch = std::stoi(value);
        else if (key == "valuesize" || key == "value_size") args.valueSizes = value;
        else throw std::invalid_argument("unknown option: --" + key);
    }
    return args;
}

std::vector<std::string> Split(const std::string &value, char delimiter)
{
    std::vector<std::string> parts;
    std::stringstream stream(value);
    std::string part;
    while (std::getline(stream, part, delimiter)) parts.push_back(part);
    return parts;
}

std::vector<rh2d_batch_data::SizeConfig> ParseSizeConfigs(const std::string &value)
{
    std::vector<rh2d_batch_data::SizeConfig> configs;
    for (const auto &part : Split(value, ',')) {
        auto fields = Split(part, ':');
        if (fields.empty() || fields[0].empty()) throw std::invalid_argument("invalid valuesize config");
        int count = fields.size() > 1 ? std::stoi(fields[1]) : -1;
        configs.push_back({std::stoull(fields[0]), count});
    }
    return configs;
}

void Validate(const Args &args, const std::vector<rh2d_batch_data::SizeConfig> &configs)
{
    if (args.count <= 0 || args.batch <= 0 || args.count % args.batch != 0) {
        throw std::invalid_argument("count and batch must be positive, and count must be divisible by batch");
    }
    int explicitCount = 0;
    bool hasRemaining = false;
    for (const auto &config : configs) {
        if (config.size == 0 || config.count == 0 || config.count < -1) {
            throw std::invalid_argument("valuesize and item count must be positive");
        }
        if (config.count == -1) hasRemaining = true;
        else explicitCount += config.count;
    }
    if (configs.empty() || (!hasRemaining && explicitCount < args.batch)) {
        throw std::invalid_argument("valuesize item counts do not fill one batch");
    }
}

void PrintUsage(const char *program)
{
    std::cout << "Usage: " << program << " [--count=N] [--batch=N] [--valuesize=CONFIG]\n"
              << "Example: " << program << " --count=256 --batch=8 --valuesize=1835008\n"
              << "Mixed sizes: --valuesize=1048576:3,4194304:2,8388608\n";
}

int Generate(const Args &args, const std::vector<rh2d_batch_data::SizeConfig> &configs)
{
    std::string signature = rh2d_batch_data::BuildSignature(args.count, args.batch, configs);
    std::string path = rh2d_batch_data::BuildCachePath(signature);
    int progressInterval = std::max(1, args.count / 20);
    rh2d_batch_data::Data data;
    std::cout << "Generating " << args.count << " key-value pairs for " << path << std::endl;
    rh2d_batch_data::Generate(args.count, args.batch, configs, data, [&](size_t generated) {
        if (generated % progressInterval == 0 || generated == static_cast<size_t>(args.count)) {
            std::cout << "Generate progress: " << generated * 100 / args.count << "% (" << generated << "/"
                      << args.count << ")" << std::endl;
        }
    });
    std::string error;
    if (!rh2d_batch_data::Save(path, signature, data, error)) {
        std::cerr << "Failed to save test data: " << error << std::endl;
        return 1;
    }
    std::cout << "Test data saved: " << path << std::endl;
    return 0;
}

}  // namespace

int main(int argc, char *argv[])
{
    try {
        Args args = ParseArgs(argc, argv);
        if (args.help) {
            PrintUsage(argv[0]);
            return 0;
        }
        auto configs = ParseSizeConfigs(args.valueSizes);
        Validate(args, configs);
        return Generate(args, configs);
    } catch (const std::exception &error) {
        std::cerr << "Error: " << error.what() << std::endl;
        PrintUsage(argv[0]);
        return 1;
    }
}

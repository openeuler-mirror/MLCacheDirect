#!/bin/bash
set -e  # 出错立即退出

# ===================== 权限自检（自动加执行权限） =====================
SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
if [ ! -x "${SCRIPT_PATH}" ]; then
    echo -e "\033[1;33m⚠️  检测到脚本无执行权限，正在自动添加...\033[0m"
    chmod +x "${SCRIPT_PATH}" && exec "${SCRIPT_PATH}" "$@" || {
        echo -e "\033[1;31m❌ 权限添加失败，请手动执行：chmod +x ${SCRIPT_PATH}\033[0m"
        exit 1
    }
fi

# ===================== 核心新增：自动识别架构 =====================
# 步骤1：识别当前系统架构（兼容uname和rpm规范）
detect_arch() {
    local arch=$(uname -m)
    case "${arch}" in
        x86_64)
            echo "x86_64"
            ;;
        aarch64|arm64)
            echo "aarch64"
            ;;
        *)
            echo -e "${RED}❌ 错误：不支持的架构 ${arch}，仅支持x86_64/aarch64！${NC}"
            exit 1
            ;;
    esac
}

# 步骤2：定义核心变量（含架构、版本）
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 默认不编译测试、不启用故障注入；仅显式传入 -t/--test 或 --gcov-ut 时只运行测试并退出
DO_TEST=0
DO_GCOV_SO=0
DO_GCOV_UT=0
WITH_INJECT=OFF
USE_BEAR=0
USE_MOCK=OFF
while [[ $# -gt 0 ]]; do
    case "$1" in
        -t|--test)
            DO_TEST=1
            shift
            ;;
        --gcov)
            DO_GCOV_SO=1
            shift
            ;;
        --gcov-ut)
            DO_TEST=1
            DO_GCOV_UT=1
            shift
            ;;
        --bear|--compile-commands)
            USE_BEAR=1
            echo -e "${YELLOW}✅ 启用bear生成compile_commands.json${NC}"
            shift
            ;;
        --with-inject)
            WITH_INJECT=ON
            echo -e "${YELLOW}✅ 启用故障注入功能${NC}"
            shift
            ;;
        --mock)
            USE_MOCK=ON
            echo -e "${YELLOW}✅ 启用URMA mock模式${NC}"
            shift
            ;;
        -h|--help)
            echo "用法: $0 [-t|--test] [--gcov] [--gcov-ut] [--with-inject] [--bear|--compile-commands] [--mock]"
            echo "  -t, --test                 只编译并运行测试"
            echo "  --gcov                     只编译生成带覆盖率插桩的libos_transport.so，不运行测试、不打包RPM"
            echo "  --gcov-ut                  编译并运行单元测试，打印src目录源码覆盖率并生成lcov结果"
            echo "  --with-inject              启用故障注入功能"
            echo "  --bear, --compile-commands 使用bear生成compile_commands.json"
            echo "  --mock                     启用URMA mock模式，使用本地模拟实现替代真实URMA SDK"
            exit 0
            ;;
        *)
            echo -e "${RED}❌ 错误：未知参数 $1。使用 -h 查看帮助。${NC}"
            exit 1
            ;;
    esac
done

if [ ${DO_GCOV_SO} -eq 1 ] && [ ${DO_TEST} -eq 1 ]; then
    echo -e "${RED}❌ 错误：--gcov 用于生成插桩版so，不能和 -t/--test/--gcov-ut 同时使用。${NC}"
    exit 1
fi

# 自动识别架构
ARCH=$(detect_arch)
echo -e "${YELLOW}🔍 检测到当前架构：${ARCH}${NC}"
echo -e "${YELLOW}🧪 运行单元测试：${DO_TEST}${NC}"
echo -e "${YELLOW}📊 覆盖率插桩SO：${DO_GCOV_SO}${NC}"
echo -e "${YELLOW}📈 单测覆盖率统计：${DO_GCOV_UT}${NC}"
echo -e "${YELLOW}💉 故障注入：${WITH_INJECT}${NC}"
echo -e "${YELLOW}🐻 生成编译命令：${USE_BEAR}${NC}"
echo -e "${YELLOW}🎭 URMA Mock模式：${USE_MOCK}${NC}"
BUILD_TYPE="Release"
if [ ${DO_TEST} -eq 1 ] || [ ${DO_GCOV_SO} -eq 1 ]; then
    BUILD_TYPE="Debug"
fi

# 版本常量（可按需修改）
PKG_NAME="os-transport"
PKG_VERSION="1.0.0"
PKG_VERSION_MAJOR="1"
PKG_RELEASE="1"

# 绝对路径（无空格）
ROOT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
BUILD_DIR="${ROOT_DIR}/build-${ARCH}"          # CMake 构建目录
BAZEL_BUILD_DIR="${ROOT_DIR}/build-bazel-${ARCH}"  # Bazel 构建目录
INSTALL_DIR="${BUILD_DIR}/install"
OUTPUT_DIR="${ROOT_DIR}/output"

# RPM spec文件路径
RPM_SPEC="${ROOT_DIR}/rpm/os-transport.spec"

# ===================== 前置校验 =====================
# 校验源码根目录和CMakeLists.txt存在
if [ ! -d "${ROOT_DIR}" ] || [ ! -f "${ROOT_DIR}/CMakeLists.txt" ]; then
    echo -e "${RED}❌ 错误：源码目录或CMakeLists.txt不存在！${NC}"
    exit 1
fi

# 校验spec文件存在（测试模式和覆盖率so模式不需要打包）
if [ ${DO_TEST} -eq 0 ] && [ ${DO_GCOV_SO} -eq 0 ] && [ ! -f "${RPM_SPEC}" ]; then
    echo -e "${RED}❌ 错误：RPM spec文件不存在 → ${RPM_SPEC}${NC}"
    exit 1
fi

# ===================== 步骤1：检查依赖 =====================
echo -e "${YELLOW}[1/6] 检查编译/打包依赖...${NC}"
deps=("cmake" "gcc" "make")
if [ ${DO_GCOV_UT} -eq 1 ]; then
    deps+=("gcov" "lcov" "genhtml")
fi
if [ ${DO_TEST} -eq 0 ] && [ ${DO_GCOV_SO} -eq 0 ]; then
    deps+=("rpmbuild" "file")
fi
if [ ${USE_BEAR} -eq 1 ]; then
    deps+=("bear")
fi
for dep in "${deps[@]}"; do
    if ! command -v "${dep}" &> /dev/null; then
        echo -e "${RED}❌ 错误：未安装 ${dep}，请先安装！${NC}"
        exit 1
    fi
done

# ===================== 步骤2：清理旧文件 =====================
echo -e "${YELLOW}[2/6] 清理旧编译和打包产物（架构：${ARCH}）...${NC}"
if [ ${DO_TEST} -eq 1 ]; then
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}" || {
        echo -e "${RED}❌ 错误：创建目录失败 → ${BUILD_DIR}${NC}"
        exit 1
    }
elif [ ${DO_GCOV_SO} -eq 1 ]; then
    rm -rf "${BUILD_DIR}"
    mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" || {
        echo -e "${RED}❌ 错误：创建目录失败 → ${BUILD_DIR}${NC}"
        exit 1
    }
else
    rm -rf "${BUILD_DIR}" "${BAZEL_BUILD_DIR}" "${OUTPUT_DIR}"
    mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${OUTPUT_DIR}" || {
        echo -e "${RED}❌ 错误：创建目录失败 → ${BUILD_DIR}${NC}"
        exit 1
    }
fi

if [ ! -d "${BUILD_DIR}" ]; then
    echo -e "${RED}❌ 错误：创建目录失败 → ${BUILD_DIR}${NC}"
    exit 1
fi

# ===================== 步骤3：CMake配置（跨架构兼容） =====================
echo -e "${YELLOW}[3/6] 执行CMake配置（架构：${ARCH}）...${NC}"
cd "${BUILD_DIR}" || {
    echo -e "${RED}❌ 错误：无法进入目录 → ${BUILD_DIR}${NC}"
    exit 1
}

CMAKE_C_FLAGS="-Wall -Wextra -O2 -fPIC"
if [ ${DO_GCOV_SO} -eq 1 ] || [ ${DO_GCOV_UT} -eq 1 ]; then
    CMAKE_C_FLAGS="-Wall -Wextra -O0 -g --coverage -fPIC"
fi

# CMake配置：无架构硬编码，适配x86_64/aarch64
cmake \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DCMAKE_BUILD_TYPE="${BUILD_TYPE}" \
    -DCMAKE_C_FLAGS="${CMAKE_C_FLAGS}" \
    -DOS_TRANSPORT_BUILD_TESTS="$([ ${DO_TEST} -eq 1 ] && echo ON || echo OFF)" \
    -DOS_TRANSPORT_WITH_INJECT="${WITH_INJECT}" \
    -DUSE_URMA_MOCK="${USE_MOCK}" \
    "${ROOT_DIR}"

print_gcov_summary_from_output() {
    local gcov_output="$1"
    local src_file="$2"

    printf "%s\n" "${gcov_output}" | awk -v src="${src_file}" -v abs="${ROOT_DIR}/${src_file}" '
        function cov_value(line) {
            sub(/^[^:]+:/, "", line)
            sub(/ of .*/, "", line)
            return line
        }
        $0 == "File '\''" src "'\''" || $0 == "File '\''" abs "'\''" || $0 ~ ("^File '\''.*" src "'\''$") {
            in_src = 1
            lines = branches = taken = calls = "n/a"
            next
        }
        in_src && /^Lines executed:/ {
            lines = cov_value($0)
            next
        }
        in_src && /^Branches executed:/ {
            branches = cov_value($0)
            next
        }
        in_src && /^Taken at least once:/ {
            taken = cov_value($0)
            next
        }
        in_src && /^Calls executed:/ {
            calls = cov_value($0)
            printf "  %-34s lines=%-8s branches=%-8s taken=%-8s calls=%s\n", src, lines, branches, taken, calls
            printed = 1
            in_src = 0
        }
        END {
            if (in_src) {
                printf "  %-34s lines=%-8s branches=%-8s taken=%-8s calls=%s\n", src, lines, branches, taken, calls
                printed = 1
            }
            if (!printed) {
                printf "  %-34s coverage data not found\n", src
            }
        }
    '
}

print_cmake_gcov_report() {
    echo -e "${YELLOW}[GCOV] src目录源码覆盖率：${NC}"

    local output
    output=$(gcov -b -c -o "${BUILD_DIR}/CMakeFiles/test_os_transport_unit.dir/test/test_os_transport_unit.c.gcno" \
        "${ROOT_DIR}/test/test_os_transport_unit.c")
    print_gcov_summary_from_output "${output}" "src/os_transport.c"

    output=$(gcov -b -c -o "${BUILD_DIR}/CMakeFiles/test_os_transport_log_unit.dir/test/test_os_transport_log_unit.c.gcno" \
        "${ROOT_DIR}/test/test_os_transport_log_unit.c")
    print_gcov_summary_from_output "${output}" "src/os_transport_log.c"

    output=$(gcov -b -c -o "${BUILD_DIR}/CMakeFiles/test_thread_pool.dir/src/os_transport_thread_pool.c.gcno" \
        "${ROOT_DIR}/src/os_transport_thread_pool.c")
    print_gcov_summary_from_output "${output}" "src/os_transport_thread_pool.c"
}

generate_cmake_lcov_report() {
    local raw_info="${BUILD_DIR}/coverage-all.info"
    local src_info="${BUILD_DIR}/coverage-src.info"
    local html_dir="${BUILD_DIR}/coverage-html"

    echo -e "${YELLOW}[LCOV] 生成src目录源码覆盖率结果：${NC}"
    lcov --capture \
        --directory "${BUILD_DIR}" \
        --output-file "${raw_info}" \
        --rc branch_coverage=1
    lcov --extract "${raw_info}" "${ROOT_DIR}/src/*" \
        --output-file "${src_info}" \
        --rc branch_coverage=1
    rm -f "${raw_info}"

    echo -e "${GREEN}📊 LCOV结果：${src_info}${NC}"
    echo -e "${YELLOW}[GENHTML] 生成HTML覆盖率报告：${NC}"
    genhtml --branch-coverage "${src_info}" -o "${html_dir}"
    echo -e "${GREEN}📈 HTML报告目录：${html_dir}${NC}"
}

if [ ${DO_TEST} -eq 1 ]; then
    echo -e "${YELLOW}[4/6] 编译并运行测试...${NC}"
    TEST_TARGETS="test_thread_pool test_os_transport_log_unit test_os_transport_unit"
    if [ "${WITH_INJECT}" = "ON" ]; then
        TEST_TARGETS="${TEST_TARGETS} test_inject_unit"
    fi
    make ${TEST_TARGETS} -j$(nproc 2>/dev/null || echo 4)
    ./test_thread_pool
    ./test_os_transport_log_unit
    ./test_os_transport_unit
    if [ "${WITH_INJECT}" = "ON" ]; then
        ./test_inject_unit
    fi
    if [ ${DO_GCOV_UT} -eq 1 ]; then
        print_cmake_gcov_report
        generate_cmake_lcov_report
    fi
    echo -e "${GREEN}✅ 测试通过。${NC}"
    exit 0
fi

if [ ${DO_GCOV_SO} -eq 1 ]; then
    echo -e "${YELLOW}[4/6] 编译生成带覆盖率插桩的libos_transport.so（架构：${ARCH}）...${NC}"
    if [ ${USE_BEAR} -eq 1 ]; then
        bear -- make os_transport -j$(nproc 2>/dev/null || echo 4)
    else
        make os_transport -j$(nproc 2>/dev/null || echo 4)
    fi

    echo -e "${YELLOW}[5/6] 临时安装到 ${INSTALL_DIR}...${NC}"
    make install DESTDIR="${INSTALL_DIR}"

    echo -e "${GREEN}✅ 覆盖率插桩版so生成成功。${NC}"
    echo -e "${GREEN}📦 构建产物：${BUILD_DIR}/libos_transport.so${NC}"
    echo -e "${GREEN}📦 安装产物：${INSTALL_DIR}/usr/lib64/libos_transport.so${NC}"
    echo -e "${YELLOW}ℹ️  使用该so运行目标程序并正常退出后，.gcda通常会写入 ${BUILD_DIR}/CMakeFiles/os_transport.dir/src/${NC}"
    exit 0
fi

# ===================== 步骤4：编译生成库 =====================
echo -e "${YELLOW}[4/6] 编译生成libos_transport.so（架构：${ARCH}）...${NC}"
if [ ${USE_BEAR} -eq 1 ]; then
    bear -- make -j$(nproc 2>/dev/null || echo 4)
else
    make -j$(nproc 2>/dev/null || echo 4)
fi

# ===================== 步骤5：临时安装 =====================
echo -e "${YELLOW}[5/6] 临时安装到 ${INSTALL_DIR}...${NC}"
make install DESTDIR="${INSTALL_DIR}"

# ===================== 步骤6：打包RPM（动态传递架构+版本） =====================
echo -e "${YELLOW}[6/6] 打包生成RPM文件（架构：${ARCH}，版本：${PKG_VERSION}）...${NC}"
rpmbuild -bb --nodeps \
    --define "_topdir ${BUILD_DIR}/rpmbuild" \
    --define "_builddir ${ROOT_DIR}" \
    --define "_rpmdir ${OUTPUT_DIR}" \
    --define "_srcrpmdir ${OUTPUT_DIR}" \
    --define "version ${PKG_VERSION}" \
    --define "version_major ${PKG_VERSION_MAJOR}" \
    --define "release ${PKG_RELEASE}" \
    --define "install_root ${INSTALL_DIR}" \
    --define "build_arch ${ARCH}" \
    "${RPM_SPEC}"

# ===================== 验证结果 =====================
# 匹配带版本+架构的RPM包
MAIN_RPM=$(find "${OUTPUT_DIR}" -name "${PKG_NAME}-${PKG_VERSION}-${PKG_RELEASE}.${ARCH}.rpm" | head -1)
DEVEL_RPM=$(find "${OUTPUT_DIR}" -name "${PKG_NAME}-devel-${PKG_VERSION}-${PKG_RELEASE}.${ARCH}.rpm" | head -1)

if [ -f "${MAIN_RPM}" ] && [ -f "${DEVEL_RPM}" ]; then
    echo -e "${GREEN}✅ 编译+打包成功（架构：${ARCH}，版本：${PKG_VERSION}）！${NC}"
    echo -e "${GREEN}📦 主包路径：${MAIN_RPM}${NC}"
    echo -e "${GREEN}📦 Devel包路径：${DEVEL_RPM}${NC}"
    # 验证包信息
    echo -e "${YELLOW}🔍 RPM包架构信息：${NC}"
    rpm -qip "${MAIN_RPM}" | grep -E "Version|Release|Architecture"
else
    echo -e "${RED}❌ 打包失败：未找到主包或devel包！${NC}"
    exit 1
fi

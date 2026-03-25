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

# 自动识别架构
ARCH=$(detect_arch)
echo -e "${YELLOW}🔍 检测到当前架构：${ARCH}${NC}"

# 版本常量（可按需修改）
PKG_NAME="os-transport"
PKG_VERSION="1.0.0"
PKG_VERSION_MAJOR="1"
PKG_RELEASE="1"

# 绝对路径（无空格）
ROOT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
BUILD_DIR="${ROOT_DIR}/build-${ARCH}"  # 按架构分build目录，避免冲突
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

# 校验spec文件存在
if [ ! -f "${RPM_SPEC}" ]; then
    echo -e "${RED}❌ 错误：RPM spec文件不存在 → ${RPM_SPEC}${NC}"
    exit 1
fi

# ===================== 步骤1：检查依赖 =====================
echo -e "${YELLOW}[1/6] 检查编译/打包依赖...${NC}"
deps=("cmake" "gcc" "rpmbuild" "make" "file")
for dep in "${deps[@]}"; do
    if ! command -v "${dep}" &> /dev/null; then
        echo -e "${RED}❌ 错误：未安装 ${dep}，请先安装！${NC}"
        exit 1
    fi
done

# ===================== 步骤2：清理旧文件 =====================
echo -e "${YELLOW}[2/6] 清理旧编译文件（架构：${ARCH}）...${NC}"
rm -rf "${BUILD_DIR}"
mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${OUTPUT_DIR}" || {
    echo -e "${RED}❌ 错误：创建目录失败 → ${BUILD_DIR}${NC}"
    exit 1
}

# ===================== 步骤3：CMake配置（跨架构兼容） =====================
echo -e "${YELLOW}[3/6] 执行CMake配置（架构：${ARCH}）...${NC}"
cd "${BUILD_DIR}" || {
    echo -e "${RED}❌ 错误：无法进入目录 → ${BUILD_DIR}${NC}"
    exit 1
}

# CMake配置：无架构硬编码，适配x86_64/aarch64
cmake \
    -DCMAKE_INSTALL_PREFIX=/usr \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_C_FLAGS="-Wall -Wextra -O2 -fPIC" \
    "${ROOT_DIR}"

# ===================== 步骤4：编译生成库 =====================
echo -e "${YELLOW}[4/6] 编译生成libos_transport.so（架构：${ARCH}）...${NC}"
make -j$(nproc 2>/dev/null || echo 4)

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
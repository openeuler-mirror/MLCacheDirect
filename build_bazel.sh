#!/bin/bash
set -e

# ===================== auto chmod =====================
SCRIPT_PATH=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/$(basename "${BASH_SOURCE[0]}")
if [ ! -x "${SCRIPT_PATH}" ]; then
    echo -e "\033[1;33m[WARN] Script not executable, adding +x ...\033[0m"
    chmod +x "${SCRIPT_PATH}" && exec "${SCRIPT_PATH}" "$@" || {
        echo -e "\033[1;31m[ERROR] chmod failed. Run: chmod +x ${SCRIPT_PATH}\033[0m"
        exit 1
    }
fi

# ===================== arch detection =====================
detect_arch() {
    local arch
    arch=$(uname -m)
    case "${arch}" in
        x86_64) echo "x86_64" ;;
        aarch64|arm64) echo "aarch64" ;;
        *)
            echo -e "${RED}[ERROR] Unsupported arch: ${arch}. Only x86_64/aarch64.${NC}"
            exit 1
            ;;
    esac
}

# ===================== variables =====================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ARCH=$(detect_arch)
echo -e "${YELLOW}[INFO] Detected arch: ${ARCH}${NC}"

PKG_NAME="os-transport"
PKG_VERSION="1.0.0"
PKG_VERSION_MAJOR="1"
PKG_RELEASE="1"

ROOT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")
BUILD_DIR="${ROOT_DIR}/build-bazel-${ARCH}"
CMAKE_BUILD_DIR="${ROOT_DIR}/build-${ARCH}"
INSTALL_DIR="${BUILD_DIR}/install"
OUTPUT_DIR="${ROOT_DIR}/output"
RPM_SPEC="${ROOT_DIR}/rpm/os-transport.spec"

# ===================== external dependency paths =====================
CUDA_INCLUDE_DIR="${CUDA_INCLUDE_DIR:-/usr/local/cuda/include}"
CUDA_LIB_DIR="${CUDA_LIB_DIR:-/usr/local/cuda/lib64}"
URMA_INCLUDE_DIR="${URMA_INCLUDE_DIR:-/usr/include}"
URMA_LIB_DIR="${URMA_LIB_DIR:-/usr/lib64}"

# ===================== usage =====================
usage() {
    echo "Usage: $0 [options]"
    echo ""
    echo "Options:"
    echo "  -c, --clean       Clean bazel cache and build artifacts"
    echo "  -t, --test        Build and run tests"
    echo "  -d, --debug       Build in debug mode"
    echo "  -r, --rpm         Build RPM packages (default when no options)"
    echo "  -h, --help        Show this help"
    echo ""
    echo "Environment overrides:"
    echo "  CUDA_INCLUDE_DIR  Default: /usr/local/cuda/include"
    echo "  CUDA_LIB_DIR      Default: /usr/local/cuda/lib64"
    echo "  URMA_INCLUDE_DIR  Default: /usr/include"
    echo "  URMA_LIB_DIR      Default: /usr/lib64"
    echo ""
    echo "Examples:"
    echo "  $0"
    echo "  $0 -t"
    echo "  $0 -d"
    echo "  CUDA_INCLUDE_DIR=/opt/cuda/include CUDA_LIB_DIR=/opt/cuda/lib64 $0"
}

DO_CLEAN=0
DO_TEST=0
DO_RPM=1
BAZEL_CONFIG="release"

while [[ $# -gt 0 ]]; do
    case "$1" in
        -c|--clean) DO_CLEAN=1; DO_RPM=0; shift ;;
        -t|--test) DO_TEST=1; DO_RPM=0; shift ;;
        -d|--debug) BAZEL_CONFIG="debug"; shift ;;
        -r|--rpm) DO_RPM=1; shift ;;
        -h|--help) usage; exit 0 ;;
        *)
            echo -e "${RED}[ERROR] Unknown option: $1${NC}"
            usage
            exit 1
            ;;
    esac
done

# ===================== prerequisites =====================
check_dep() {
    if ! command -v "$1" &>/dev/null; then
        echo -e "${RED}[ERROR] Missing dependency: $1${NC}"
        exit 1
    fi
}

check_dir() {
    local dir="$1"
    local name="$2"
    if [ ! -d "${dir}" ]; then
        echo -e "${RED}[ERROR] ${name} not found: ${dir}${NC}"
        exit 1
    fi
}

echo -e "${YELLOW}[1/6] Checking dependencies...${NC}"
check_dep bazel
check_dep gcc

if [ ${DO_RPM} -eq 1 ]; then
    check_dep rpmbuild
    check_dep file
    if [ ! -f "${RPM_SPEC}" ]; then
        echo -e "${RED}[ERROR] RPM spec not found: ${RPM_SPEC}${NC}"
        exit 1
    fi
fi

check_dir "${CUDA_INCLUDE_DIR}" "CUDA include directory"
check_dir "${CUDA_LIB_DIR}" "CUDA lib directory"
check_dir "${URMA_INCLUDE_DIR}" "URMA include directory"
check_dir "${URMA_LIB_DIR}" "URMA lib directory"

echo -e "${YELLOW}[INFO] CUDA_INCLUDE_DIR=${CUDA_INCLUDE_DIR}${NC}"
echo -e "${YELLOW}[INFO] CUDA_LIB_DIR=${CUDA_LIB_DIR}${NC}"
echo -e "${YELLOW}[INFO] URMA_INCLUDE_DIR=${URMA_INCLUDE_DIR}${NC}"
echo -e "${YELLOW}[INFO] URMA_LIB_DIR=${URMA_LIB_DIR}${NC}"

# ===================== clean =====================
if [ ${DO_CLEAN} -eq 1 ]; then
    echo -e "${YELLOW}[INFO] Cleaning bazel cache and build artifacts...${NC}"
    cd "${ROOT_DIR}"
    bazel clean --enable_workspace 2>/dev/null || true
    rm -rf "${ROOT_DIR}/build-bazel-"* "${ROOT_DIR}/build-"* "${OUTPUT_DIR}"
    echo -e "${GREEN}[OK] Clean done.${NC}"
    exit 0
fi

# ===================== common bazel flags =====================
BAZEL_FLAGS=(
    "--config=${BAZEL_CONFIG}"
    "--enable_workspace"
)

# ===================== test =====================
if [ ${DO_TEST} -eq 1 ]; then
    echo -e "${YELLOW}[INFO] Building and running tests...${NC}"
    cd "${ROOT_DIR}"
    bazel test "${BAZEL_FLAGS[@]}" \
        //:test_thread_pool \
        //:test_os_transport_unit \
        --test_output=all 2>&1
    echo -e "${GREEN}[OK] Tests done.${NC}"
    exit 0
fi

# ===================== build library =====================
echo -e "${YELLOW}[2/6] Cleaning old build and packaging artifacts (arch: ${ARCH})...${NC}"
rm -rf "${BUILD_DIR}" "${CMAKE_BUILD_DIR}" "${OUTPUT_DIR}"
mkdir -p "${BUILD_DIR}" "${INSTALL_DIR}" "${OUTPUT_DIR}"

echo -e "${YELLOW}[3/6] Building libos_transport.so with Bazel (config: ${BAZEL_CONFIG})...${NC}"
cd "${ROOT_DIR}"
bazel build "${BAZEL_FLAGS[@]}" //:libos_transport.so 2>&1

# ===================== install layout =====================
echo -e "${YELLOW}[4/6] Creating install layout at ${INSTALL_DIR}...${NC}"
SO_FILE=$(find "$(bazel info bazel-bin 2>/dev/null)" -name "libos_transport.so" -type f | head -1)

if [ ! -f "${SO_FILE}" ]; then
    echo -e "${RED}[ERROR] libos_transport.so not found in bazel-bin${NC}"
    exit 1
fi

mkdir -p "${INSTALL_DIR}/usr/lib64"
cp "${SO_FILE}" "${INSTALL_DIR}/usr/lib64/libos_transport.so.${PKG_VERSION}"
ln -sf "libos_transport.so.${PKG_VERSION}" "${INSTALL_DIR}/usr/lib64/libos_transport.so.${PKG_VERSION_MAJOR}"
ln -sf "libos_transport.so.${PKG_VERSION}" "${INSTALL_DIR}/usr/lib64/libos_transport.so"

mkdir -p "${INSTALL_DIR}/usr/include/os-transport"
cp "${ROOT_DIR}/include/os_transport.h" "${INSTALL_DIR}/usr/include/os-transport/"

# ===================== RPM packaging =====================
if [ ${DO_RPM} -eq 1 ]; then
    echo -e "${YELLOW}[5/6] Building RPM packages (arch: ${ARCH}, version: ${PKG_VERSION})...${NC}"
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

    echo -e "${YELLOW}[6/6] Verifying RPM output...${NC}"
    MAIN_RPM=$(find "${OUTPUT_DIR}" -name "${PKG_NAME}-${PKG_VERSION}-${PKG_RELEASE}.${ARCH}.rpm" | head -1)
    DEVEL_RPM=$(find "${OUTPUT_DIR}" -name "${PKG_NAME}-devel-${PKG_VERSION}-${PKG_RELEASE}.${ARCH}.rpm" | head -1)

    if [ -f "${MAIN_RPM}" ] && [ -f "${DEVEL_RPM}" ]; then
        echo -e "${GREEN}[OK] Build + RPM packaging succeeded (arch: ${ARCH}, version: ${PKG_VERSION})${NC}"
        echo -e "${GREEN}  Main RPM:  ${MAIN_RPM}${NC}"
        echo -e "${GREEN}  Devel RPM: ${DEVEL_RPM}${NC}"
        echo -e "${YELLOW}[INFO] RPM details:${NC}"
        rpm -qip "${MAIN_RPM}" | grep -E "Version|Release|Architecture"
    else
        echo -e "${RED}[ERROR] RPM packaging failed: packages not found!${NC}"
        exit 1
    fi
else
    echo -e "${GREEN}[OK] libos_transport.so built successfully.${NC}"
    echo -e "${GREEN}  Output: ${SO_FILE}${NC}"
fi
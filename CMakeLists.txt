cmake_minimum_required(VERSION 3.10)
project(OS_TRANSPORT VERSION 1.0.0 LANGUAGES C)
option(OS_TRANSPORT_BUILD_TESTS "Build OS_TRANSPORT test binaries" OFF)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
# 关闭隐式函数声明警告
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-implicit-function-declaration")

# 收集所有源码
file(GLOB SRC_FILES src/*.c)

# 查找 URMA 库
find_library(URMA_LIB urma
    HINTS /usr/lib64 /usr/local/lib /opt/urma/lib /opt/urma/lib64
    DOC "URMA library path"
)
if(NOT URMA_LIB)
    message(WARNING "URMA library not found. Please ensure liburma.so is installed.")
else()
    message(STATUS "Found URMA library: ${URMA_LIB}")
endif()

# 查找 CUDA 运行时库
find_library(CUDART_LIB cudart
    HINTS /usr/local/cuda/lib64 /usr/lib/x86_64-linux-gnu /usr/lib64
    DOC "CUDA runtime library path"
)
if(NOT CUDART_LIB)
    message(WARNING "CUDA runtime library not found. Please ensure CUDA is installed.")
else()
    message(STATUS "Found CUDA runtime library: ${CUDART_LIB}")
endif()

# 编译动态库
add_library(os_transport SHARED
    ${SRC_FILES}
)

# 链接pthread、urma、cuda
target_link_libraries(os_transport
    pthread
    ${URMA_LIB}
    ${CUDART_LIB}
)

# 如果未找到 URMA 或 CUDA，可允许继续构建（因为测试模式可能不需要），但这里不强制
if(NOT URMA_LIB OR NOT CUDART_LIB)
    message(WARNING "Missing required libraries. The built library may have undefined symbols.")
endif()

target_include_directories(os_transport
    PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

set_target_properties(os_transport PROPERTIES
    VERSION ${PROJECT_VERSION}
    SOVERSION ${PROJECT_VERSION_MAJOR}
    OUTPUT_NAME "os_transport"
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
)

# 安装规则（对外仅导出 os_transport.h）
install(
    TARGETS os_transport
    LIBRARY DESTINATION lib64
    COMPONENT runtime
)

install(
    FILES include/os_transport.h
    DESTINATION include/os-transport
    COMPONENT devel
)

if(OS_TRANSPORT_BUILD_TESTS)
    # 编译测试程序
    add_executable(test_thread_pool test/test_thread_pool.c)
    target_link_libraries(test_thread_pool os_transport pthread)

    # os_transport.c 白盒单元测试（使用mock隔离外部依赖）
    add_executable(test_os_transport_unit test/test_os_transport_unit.c)
    target_include_directories(test_os_transport_unit
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_link_libraries(test_os_transport_unit pthread)
endif()
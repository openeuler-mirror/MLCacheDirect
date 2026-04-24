cmake_minimum_required(VERSION 3.10)
project(OS_TRANSPORT VERSION 1.0.0 LANGUAGES C)
option(OS_TRANSPORT_BUILD_TESTS "Build OS_TRANSPORT test binaries" OFF)
option(OS_TRANSPORT_WITH_INJECT "Enable fault injection for os_transport" OFF)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)
# 关闭隐式函数声明警告
# set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-implicit-function-declaration")

# 启用故障注入（可选）
if(OS_TRANSPORT_WITH_INJECT)
    add_definitions(-DOS_TRANSPORT_WITH_INJECT=1)
    message(STATUS "Fault injection enabled for os_transport (with shared memory)")
endif()

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

# 编译动态库
add_library(os_transport SHARED
    ${SRC_FILES}
)

# 链接pthread、urma；启用故障注入时额外链接rt（共享内存需要）
target_link_libraries(os_transport pthread)
if(OS_TRANSPORT_WITH_INJECT)
    target_link_libraries(os_transport rt)
endif()
if(URMA_LIB)
    target_link_libraries(os_transport ${URMA_LIB})
endif()

# 共享库在链接期就必须解析完全部符号，避免把 undefined symbol 留到 dlopen 阶段才暴露
target_link_options(os_transport PRIVATE -Wl,-z,defs)

# 如果未找到 URMA，可允许继续构建（因为测试模式可能不需要），但这里不强制
if(NOT URMA_LIB)
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

# 安装规则（默认导出 os_transport.h，启用注入时额外导出注入头文件）
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

if(OS_TRANSPORT_WITH_INJECT)
    # 故障注入头文件安装
    install(
        FILES include/os_transport_inject.h
        DESTINATION include/os-transport
        COMPONENT devel
    )
endif()

if(OS_TRANSPORT_BUILD_TESTS)
    # 故障注入单元测试（不依赖 URMA/CUDA，可以独立运行）
    if(OS_TRANSPORT_WITH_INJECT)
        add_executable(test_inject_unit test/test_inject_unit.c src/os_transport_inject.c)
        target_include_directories(test_inject_unit
            PRIVATE
                ${CMAKE_CURRENT_SOURCE_DIR}/include
        )
        target_link_libraries(test_inject_unit pthread rt)
        message(STATUS "Fault injection unit test will be built: test_inject_unit")
    endif()

    # 编译测试程序
    add_executable(test_thread_pool
        test/test_thread_pool.c
        src/os_transport_thread_pool.c
        src/os_transport_log.c
    )
    target_include_directories(test_thread_pool
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_link_libraries(test_thread_pool pthread)

    # os_transport.c 白盒单元测试（使用mock隔离外部依赖）
    add_executable(test_os_transport_unit test/test_os_transport_unit.c)
    target_include_directories(test_os_transport_unit
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_link_libraries(test_os_transport_unit pthread)
endif()

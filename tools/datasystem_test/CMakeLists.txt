cmake_minimum_required(VERSION 3.18)
project(pipeline_h2d_project LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# deps
find_package(CUDAToolkit REQUIRED)
execute_process(
    COMMAND sh -c "pip show openyuanrong-datasystem | grep '^Location:' | awk '{print $2}'"
    OUTPUT_VARIABLE OPENYUANRONG_SITE_PACKAGES
    OUTPUT_STRIP_TRAILING_WHITESPACE
    RESULT_VARIABLE PIP_SHOW_PATH_RESULT
)
if(NOT PIP_SHOW_PATH_RESULT EQUAL 0 OR NOT OPENYUANRONG_SITE_PACKAGES)
    message(FATAL_ERROR "Failed to get openyuanrong-datasystem path from pip show")
endif()
set(OPENYUANRONG_DATASYSTEM_PATH "${OPENYUANRONG_SITE_PACKAGES}/yr")

# target
add_executable(pipeline_h2d pipeline_h2d.cpp)

# debug mode
target_compile_options(pipeline_h2d PRIVATE -g -O0)

# header
file(GLOB_RECURSE
    DATASYSTEM_INCLUDE_PATH "${OPENYUANRONG_DATASYSTEM_PATH}/*/kv_client.h"
)
get_filename_component(
    DATASYSTEM_INCLUDE_DIR "${DATASYSTEM_INCLUDE_PATH}"
    DIRECTORY
)
target_include_directories(pipeline_h2d PRIVATE
    ${DATASYSTEM_INCLUDE_DIR}/..
)

# lib
file(GLOB_RECURSE
    DATASYSTEM_LIB_PATH "${OPENYUANRONG_DATASYSTEM_PATH}/*/libdatasystem.so"
)
target_link_libraries(pipeline_h2d PRIVATE 
    ${DATASYSTEM_LIB_PATH}
    CUDA::cudart
)

# fault injection H2D test
add_executable(pipeline_h2d_fault_test pipeline_h2d_fault_test.cpp)
target_compile_options(pipeline_h2d_fault_test PRIVATE -g -O0 -Wall)
target_include_directories(pipeline_h2d_fault_test PRIVATE
    ${DATASYSTEM_INCLUDE_DIR}/..
)
target_link_libraries(pipeline_h2d_fault_test PRIVATE
    ${DATASYSTEM_LIB_PATH}
    CUDA::cudart
    dl
)

# standalone injection CLI
add_executable(mlcd_inject_cli
    mlcd_inject_cli.c
    ${CMAKE_CURRENT_SOURCE_DIR}/../../src/os_transport_inject.c
)
target_compile_definitions(mlcd_inject_cli PRIVATE OS_TRANSPORT_WITH_INJECT=1)
target_compile_options(mlcd_inject_cli PRIVATE -Wall -Wextra -g -O0)
target_include_directories(mlcd_inject_cli PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../../include
)
target_link_options(mlcd_inject_cli PRIVATE -static)
target_link_libraries(mlcd_inject_cli PRIVATE
    pthread
    rt
)

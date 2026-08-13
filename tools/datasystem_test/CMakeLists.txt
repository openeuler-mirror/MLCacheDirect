cmake_minimum_required(VERSION 3.18)
project(pipeline_h2d_project LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Check if using mock mode
option(USE_CUDA_MOCK "Use mock mode without CUDA" OFF)

# deps
if(NOT USE_CUDA_MOCK)
    find_package(CUDAToolkit REQUIRED)
endif()
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

# header
file(GLOB_RECURSE
    DATASYSTEM_INCLUDE_PATH "${OPENYUANRONG_DATASYSTEM_PATH}/*/kv_client.h"
)
get_filename_component(
    DATASYSTEM_INCLUDE_DIR "${DATASYSTEM_INCLUDE_PATH}"
    DIRECTORY
)
set(COMMON_HEADER ${DATASYSTEM_INCLUDE_DIR}/..)

# lib
file(GLOB_RECURSE
    DATASYSTEM_LIB_PATH "${OPENYUANRONG_DATASYSTEM_PATH}/*/libdatasystem.so"
)
if(USE_CUDA_MOCK)
    set(COMMON_LIB ${DATASYSTEM_LIB_PATH})
else()
    set(COMMON_LIB ${DATASYSTEM_LIB_PATH} CUDA::cudart)
endif()

#################### feature test util ##################

add_executable(pipeline_h2d pipeline_h2d.cpp)
target_include_directories(pipeline_h2d PRIVATE
    ${COMMON_HEADER}/
)
target_link_libraries(pipeline_h2d PRIVATE 
    ${COMMON_LIB}
)
target_compile_options(pipeline_h2d PRIVATE
    $<$<CONFIG:Debug>:-O0;-g;-fno-omit-frame-pointer>;-Wall
)

add_executable(pipeline_async_pin_test pipeline_async_pin_test.cpp)
target_include_directories(pipeline_async_pin_test PRIVATE
    ${COMMON_HEADER}/
)
target_link_libraries(pipeline_async_pin_test PRIVATE
    ${COMMON_LIB}
)
target_compile_options(pipeline_async_pin_test PRIVATE
    $<$<CONFIG:Debug>:-O0;-g;-fno-omit-frame-pointer>;-Wall
)


#################### batch test util ##################

add_executable(pipeline_rh2d_batch_test pipeline_rh2d_batch_test.cpp)
target_include_directories(pipeline_rh2d_batch_test PRIVATE
    ${COMMON_HEADER}/
    $<$<BOOL:${USE_CUDA_MOCK}>:${CMAKE_CURRENT_SOURCE_DIR}/../../mock>
)
target_link_libraries(pipeline_rh2d_batch_test PRIVATE
    ${COMMON_LIB}
)
if(USE_CUDA_MOCK)
    target_compile_definitions(pipeline_rh2d_batch_test PRIVATE USE_CUDA_MOCK)
    target_compile_definitions(pipeline_rh2d_batch_test PRIVATE USE_URMA_MOCK)
endif()
target_compile_options(pipeline_rh2d_batch_test PRIVATE
    $<$<CONFIG:Debug>:-O0;-g;-fno-omit-frame-pointer>;-Wall
)

add_executable(pipeline_rh2d_batch_data_generator pipeline_rh2d_batch_data_generator.cpp)
target_compile_options(pipeline_rh2d_batch_data_generator PRIVATE
    $<$<CONFIG:Debug>:-O0;-g;-fno-omit-frame-pointer>;-Wall
)

#################### fault injection test util ##################

add_executable(pipeline_h2d_fault_test pipeline_h2d_fault_test.cpp)
target_include_directories(pipeline_h2d_fault_test PRIVATE
    ${COMMON_HEADER}/
)
target_link_libraries(pipeline_h2d_fault_test PRIVATE 
    ${COMMON_LIB}
    dl
)
target_compile_options(pipeline_h2d_fault_test PRIVATE
    $<$<CONFIG:Debug>:-O0;-g;-fno-omit-frame-pointer>;-Wall
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

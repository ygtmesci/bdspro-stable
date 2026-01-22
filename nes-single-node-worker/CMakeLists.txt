# After existing find_package calls or in target_link_libraries:

find_package(etcd-cpp-api CONFIG REQUIRED)

add_subdirectory(src)
include(GNUInstallDirs)

# Add nes-worker library
get_source(nes-single-node-worker NES_WORKER_SOURCE_FILES)
add_library(nes-single-node-worker-lib ${NES_WORKER_SOURCE_FILES})

target_include_directories(nes-single-node-worker-lib PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include/nebulastream/>)

target_link_libraries(nes-single-node-worker-lib
        PUBLIC
        nes-grpc
        nes-query-compiler
        nes-query-optimizer
        nes-single-node-worker-interface
        PRIVATE
        etcd-cpp-api
)

add_executable(nes-single-node-worker src/SingleNodeWorkerStarter.cpp)
target_link_libraries(nes-single-node-worker PRIVATE nes-single-node-worker-lib)
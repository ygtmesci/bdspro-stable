# After existing find_package calls or in target_link_libraries:

find_package(etcd-cpp-api CONFIG REQUIRED)

target_link_libraries(nes-single-node-worker-lib
        PUBLIC
        nes-grpc
        nes-query-compiler
        nes-query-optimizer
        nes-single-node-worker-interface
        PRIVATE
        etcd-cpp-api
)
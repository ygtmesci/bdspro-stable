#pragma once
#include <utility>
#include <memory>
#include <SingleNodeWorker.hpp>
#include <SingleNodeWorkerRPCService.grpc.pb.h>

namespace NES
{
/**
 * @brief GRPC Interface to interact with the SingleNodeWorker. 
 */
class GRPCServer final : public WorkerRPCService::Service
{
public:
    grpc::Status RegisterQuery(grpc::ServerContext*, const RegisterQueryRequest*, RegisterQueryReply*) override;
    grpc::Status UnregisterQuery(grpc::ServerContext*, const UnregisterQueryRequest*, google::protobuf::Empty*) override;
    grpc::Status StartQuery(grpc::ServerContext*, const StartQueryRequest*, google::protobuf::Empty*) override;
    grpc::Status StopQuery(grpc::ServerContext*, const StopQueryRequest*, google::protobuf::Empty*) override;
    grpc::Status RequestQueryStatus(grpc::ServerContext*, const QueryStatusRequest*, QueryStatusReply*) override;
    grpc::Status RequestQueryLog(grpc::ServerContext* context, const QueryLogRequest* request, QueryLogReply* response) override;
    grpc::Status RequestStatus(grpc::ServerContext* context, const WorkerStatusRequest* request, WorkerStatusResponse* response) override;

    explicit GRPCServer(std::unique_ptr<SingleNodeWorker> delegate) : delegate(std::move(delegate)) { }

private:
    std::unique_ptr<SingleNodeWorker> delegate;
};
}
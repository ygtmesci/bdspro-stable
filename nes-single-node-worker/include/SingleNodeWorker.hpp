#pragma once

#include <chrono>
#include <expected>
#include <memory>
#include <optional>
#include <string>
#include <Identifiers/Identifiers.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngine.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Pointers.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

#include <WorkerState/WorkerQueryPlanStore.h>

namespace NES
{

class EtcdWorkerReconciler; // Forward declaration

class SingleNodeWorker
{
    SharedPtr<CompositeStatisticListener> listener;
    SharedPtr<NodeEngine> nodeEngine;
    UniquePtr<QueryOptimizer> optimizer;
    UniquePtr<QueryCompilation::QueryCompiler> compiler;
    UniquePtr<WorkerQueryPlanStore> planStore;
    UniquePtr<EtcdWorkerReconciler> reconciler;
    SingleNodeWorkerConfiguration configuration;

public:
    explicit SingleNodeWorker(const SingleNodeWorkerConfiguration&, WorkerId = WorkerId("SingleNodeWorker"));
    ~SingleNodeWorker();
    
    SingleNodeWorker(const SingleNodeWorker& other) = delete;
    SingleNodeWorker& operator=(const SingleNodeWorker& other) = delete;
    SingleNodeWorker(SingleNodeWorker&& other) noexcept;
    SingleNodeWorker& operator=(SingleNodeWorker&& other) noexcept;

    [[nodiscard]] std::expected<LocalQueryId, Exception> registerQuery(LogicalPlan plan) noexcept;
    std::expected<void, Exception> startQuery(LocalQueryId queryId) noexcept;
    std::expected<void, Exception> stopQuery(LocalQueryId queryId, QueryTerminationType terminationType) noexcept;
    std::expected<void, Exception> unregisterQuery(LocalQueryId queryId) noexcept;

    [[nodiscard]] std::optional<QueryLog::Log> getQueryLog(LocalQueryId queryId) const;
    [[nodiscard]] std::expected<LocalQueryStatus, Exception> getQueryStatus(LocalQueryId queryId) const noexcept;
    [[nodiscard]] WorkerStatus getWorkerStatus(std::chrono::system_clock::time_point after) const;

private:
    /// Called by reconciler when new query discovered in etcd
    /// @param distributedQueryId The distributed query ID (horse name like "bold_appaloosa")
    /// @param plan The logical plan to execute
    void onQueryDiscoveredFromEtcd(const std::string& distributedQueryId, LogicalPlan plan);
};
}
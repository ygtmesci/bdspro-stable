#include <SingleNodeWorker.hpp>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <limits>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <unistd.h>

#include <Identifiers/Identifiers.hpp>
#include <Identifiers/NESStrongType.hpp>
#include <Identifiers/NESStrongTypeFormat.hpp>
#include <Listeners/QueryLog.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/NodeEngineBuilder.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Pointers.hpp>
#include <Util/UUID.hpp>

#include <cpptrace/from_current.hpp>
#include <CompositeStatisticListener.hpp>
#include <ErrorHandling.hpp>
#include <GoogleEventTracePrinter.hpp>
#include <QueryCompiler.hpp>
#include <QueryOptimizer.hpp>
#include <SingleNodeWorkerConfiguration.hpp>
#include <WorkerStatus.hpp>

#include <WorkerState/FileWorkerQueryPlanStore.h>
#include <Reconciler/EtcdWorkerReconciler.hpp>

extern void initReceiverService(const std::string& connectionAddr, const NES::WorkerId& workerId);
extern void initSenderService(const std::string& connectionAddr, const NES::WorkerId& workerId);

namespace NES
{

SingleNodeWorker::~SingleNodeWorker() = default;

SingleNodeWorker::SingleNodeWorker(const SingleNodeWorkerConfiguration& config, WorkerId workerId)
    : listener(std::make_shared<CompositeStatisticListener>()), configuration(config)
{
    if (configuration.enableGoogleEventTrace.getValue())
    {
        auto googleTracePrinter = std::make_shared<GoogleEventTracePrinter>(
            fmt::format("trace_{}_{:%Y-%m-%d_%H-%M-%S}_{:d}.json",
                        workerId.getRawValue(),
                        std::chrono::system_clock::now(),
                        ::getpid()));
        googleTracePrinter->start();
        listener->addListener(googleTracePrinter);
    }

    nodeEngine = NodeEngineBuilder(configuration.workerConfiguration, copyPtr(listener)).build(workerId);
    if (!nodeEngine) {
        throw std::runtime_error("SingleNodeWorker: Failed to build NodeEngine.");
    }

    optimizer = std::make_unique<QueryOptimizer>(this->configuration.workerConfiguration.defaultQueryExecution);
    if (!optimizer) {
        throw std::runtime_error("SingleNodeWorker: Failed to initialize QueryOptimizer.");
    }
    
    compiler = std::make_unique<QueryCompilation::QueryCompiler>();
    if (!compiler) {
        throw std::runtime_error("SingleNodeWorker: Failed to initialize QueryCompiler.");
    }

    // Worker-side query plan store init
    if (!configuration.queryPlanStoreDir.getValue().empty())
    {
        const auto baseDir = std::filesystem::path(configuration.queryPlanStoreDir.getValue());
        const auto workerDir = baseDir / fmt::format("{}", workerId);

        std::error_code ec;
        std::filesystem::create_directories(workerDir, ec);
        if (ec)
        {
            NES_ERROR("Could not create query plan store dir '{}': {}", workerDir.string(), ec.message());
            planStore = nullptr;
        }
        else
        {
            planStore = std::make_unique<FileWorkerQueryPlanStore>(baseDir);
        }
    }
    else
    {
        planStore = nullptr;
    }

    // Recovery from local plan store
    if (planStore)
    {
        NES_INFO("SingleNodeWorker: attempting query plan recovery from '{}'", configuration.queryPlanStoreDir.getValue());

        const auto restored = planStore->loadAll();
        if (!restored)
        {
            NES_ERROR("SingleNodeWorker: query plan recovery failed: {}", restored.error().what());
        }
        else
        {
            NES_INFO("SingleNodeWorker: found {} persisted plan(s)", restored->size());

            for (const auto& [localId, storedPlan] : restored.value())
            {
                LogicalPlan planCopy = storedPlan;
                if (planCopy.getQueryId() == INVALID_LOCAL_QUERY_ID)
                {
                    planCopy.setQueryId(localId);
                }
                else if (planCopy.getQueryId() != localId)
                {
                    NES_ERROR("SingleNodeWorker: skipping restore: plan id {} != stored id {}", planCopy.getQueryId(), localId);
                    continue;
                }

                const auto res = registerQuery(std::move(planCopy));
                if (!res)
                {
                    NES_ERROR("SingleNodeWorker: failed to re-register restored plan {}: {}", localId, res.error().what());
                }
                else
                {
                    NES_INFO("SingleNodeWorker: restored local query {}", *res);
                }
            }
        }
    }

    // etcd Reconciler setup (optional)
    if (configuration.enableEtcdReconciler.getValue())
    {
        int pollMs = std::stoi(configuration.etcdPollIntervalMs.getValue());
        EtcdReconcilerConfig reconcilerConfig{
            .endpoints = configuration.etcdEndpoints.getValue(),
            .keyPrefix = configuration.etcdKeyPrefix.getValue(),
            .pollInterval = std::chrono::milliseconds(pollMs)
        };

        auto callback = [this](const std::string& distributedQueryId, LogicalPlan plan) {
            onQueryDiscoveredFromEtcd(distributedQueryId, std::move(plan));
        };

        reconciler = std::make_unique<EtcdWorkerReconciler>(
            reconcilerConfig,
            configuration.grpcAddressUri.getValue().toString(),
            callback);

        reconciler->start();
        NES_INFO("SingleNodeWorker: etcd reconciler started");
    }

    if (!configuration.connection.getValue().empty())
    {
        initReceiverService(configuration.connection.getValue().toString(), workerId);
        initSenderService(configuration.connection.getValue().toString(), workerId);
    }
}

void SingleNodeWorker::onQueryDiscoveredFromEtcd(const std::string& distributedQueryId, LogicalPlan plan)
{
    NES_INFO("SingleNodeWorker: processing distributed query '{}' from etcd", distributedQueryId);

    if (plan.getRootOperators().empty()) {
        NES_ERROR("Deserialized plan for distributed query '{}' has no root operators!", distributedQueryId);
        return;
    }

    auto registerResult = registerQuery(std::move(plan));
    if (!registerResult)
    {
        NES_ERROR("SingleNodeWorker: failed to register distributed query '{}' from etcd: {}",
                  distributedQueryId, registerResult.error().what());
        return;
    }

    LocalQueryId localQueryId = *registerResult;
    NES_INFO("SingleNodeWorker: registered distributed query '{}' as local query {}", 
             distributedQueryId, localQueryId);

    auto startResult = startQuery(localQueryId);
    if (!startResult)
    {
        NES_ERROR("SingleNodeWorker: failed to start local query {} (distributed: '{}'): {}",
                  localQueryId, distributedQueryId, startResult.error().what());
    }
    else
    {
        NES_INFO("SingleNodeWorker: started local query {} (distributed: '{}')", 
                 localQueryId, distributedQueryId);
    }
}

std::expected<LocalQueryId, Exception> SingleNodeWorker::registerQuery(LogicalPlan plan) noexcept
{
    CPPTRACE_TRY
    {
        if (plan.getQueryId() == INVALID_LOCAL_QUERY_ID)
        {
            plan.setQueryId(LocalQueryId(generateUUID()));
        }

        // Persist logical plan BEFORE compilation/registration
        if (planStore)
        {
            const auto persisted = planStore->persist(plan.getQueryId(), plan);
            if (!persisted)
            {
                return std::unexpected(persisted.error());
            }
        }

        const LogContext context("queryId", plan.getQueryId());

        auto queryPlan = optimizer->optimize(plan);
        listener->onEvent(SubmitQuerySystemEvent{plan.getQueryId(), explain(plan, ExplainVerbosity::Debug)});
        auto request = std::make_unique<QueryCompilation::QueryCompilationRequest>(queryPlan);
        request->dumpCompilationResult = configuration.workerConfiguration.dumpQueryCompilationIntermediateRepresentations.getValue();
        auto result = compiler->compileQuery(std::move(request));
        INVARIANT(result, "expected successful query compilation or exception, but got nothing");
        nodeEngine->registerCompiledQueryPlan(plan.getQueryId(), std::move(result));
        return plan.getQueryId();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::startQuery(LocalQueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_LOCAL_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->startQuery(queryId);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::stopQuery(LocalQueryId queryId, QueryTerminationType type) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_LOCAL_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->stopQuery(queryId, type);
        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected{wrapExternalException()};
    }
    std::unreachable();
}

std::expected<void, Exception> SingleNodeWorker::unregisterQuery(LocalQueryId queryId) noexcept
{
    CPPTRACE_TRY
    {
        PRECONDITION(queryId != INVALID_LOCAL_QUERY_ID, "QueryId must be not invalid!");
        nodeEngine->unregisterQuery(queryId);

        // Remove persisted logical plan (best-effort)
        if (planStore)
        {
            const auto erased = planStore->erase(queryId);
            if (!erased)
            {
                NES_ERROR("SingleNodeWorker: failed to erase persisted plan {}: {}", queryId, erased.error().what());
            }
        }

        return {};
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

std::expected<LocalQueryStatus, Exception> SingleNodeWorker::getQueryStatus(LocalQueryId queryId) const noexcept
{
    CPPTRACE_TRY
    {
        auto status = nodeEngine->getQueryLog()->getQueryStatus(queryId);
        if (not status.has_value())
        {
            return std::unexpected{QueryNotFound("{}", queryId)};
        }
        return status.value();
    }
    CPPTRACE_CATCH(...)
    {
        return std::unexpected(wrapExternalException());
    }
    std::unreachable();
}

WorkerStatus SingleNodeWorker::getWorkerStatus(std::chrono::system_clock::time_point after) const
{
    const std::chrono::system_clock::time_point until = std::chrono::system_clock::now();
    const auto summaries = nodeEngine->getQueryLog()->getStatus();
    WorkerStatus status;
    status.after = after;
    status.until = until;

    for (const auto& [queryId, state, metrics] : summaries)
    {
        switch (state)
        {
            case QueryState::Registered:
                break;

            case QueryState::Started:
                INVARIANT(metrics.start.has_value(), "If query is started, it should have a start timestamp");
                if (metrics.start.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, std::nullopt);
                }
                break;

            case QueryState::Running: {
                INVARIANT(metrics.running.has_value(), "If query is running, it should have a running timestamp");
                if (metrics.running.value() >= after)
                {
                    status.activeQueries.emplace_back(queryId, metrics.running.value());
                }
                break;
            }

            case QueryState::Stopped: {
                INVARIANT(metrics.running.has_value(), "If query is stopped, it should have a running timestamp");
                INVARIANT(metrics.stop.has_value(), "If query is stopped, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }

            case QueryState::Failed: {
                INVARIANT(metrics.stop.has_value(), "If query has failed, it should have a stopped timestamp");
                if (metrics.stop.value() >= after)
                {
                    status.terminatedQueries.emplace_back(queryId, metrics.running, metrics.stop.value(), metrics.error);
                }
                break;
            }
        }
    }

    return status;
}

std::optional<QueryLog::Log> SingleNodeWorker::getQueryLog(LocalQueryId queryId) const
{
    return nodeEngine->getQueryLog()->getLogForQuery(queryId);
}

} // namespace NES
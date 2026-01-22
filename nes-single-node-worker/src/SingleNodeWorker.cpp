// Add include at top:
#include <Reconciler/EtcdWorkerReconciler.hpp>

// In constructor, after existing recovery code and before initReceiverService:

    // -------------------------------
    // etcd Reconciler setup (optional)
    // -------------------------------
    if (configuration.enableEtcdReconciler.getValue())
    {
        EtcdReconcilerConfig reconcilerConfig{
            .endpoints = configuration.etcdEndpoints.getValue(),
            .keyPrefix = configuration.etcdKeyPrefix.getValue(),
            .pollInterval = std::chrono::milliseconds(configuration.etcdPollIntervalMs.getValue())
        };

        // Mark already-registered queries as known to avoid duplicates
        auto callback = [this](LocalQueryId queryId, LogicalPlan plan) {
            onQueryDiscoveredFromEtcd(queryId, std::move(plan));
        };

        reconciler = std::make_unique<EtcdWorkerReconciler>(
            reconcilerConfig,
            configuration.grpcAddressUri.getValue().toString(),
            callback);

        // Mark queries from local recovery as known
        if (planStore)
        {
            auto restored = planStore->loadAll();
            if (restored)
            {
                for (const auto& [id, _] : *restored)
                {
                    reconciler->markQueryAsKnown(id);
                }
            }
        }

        reconciler->start();
        NES_INFO("SingleNodeWorker: etcd reconciler started");
    }

// Add this method at the end of the file:

void SingleNodeWorker::onQueryDiscoveredFromEtcd(LocalQueryId queryId, LogicalPlan plan)
{
    NES_INFO("SingleNodeWorker: processing query {} from etcd", queryId);

    // Set the query ID if not already set
    if (plan.getQueryId() == INVALID_LOCAL_QUERY_ID)
    {
        plan.setQueryId(queryId);
    }

    auto registerResult = registerQuery(std::move(plan));
    if (!registerResult)
    {
        NES_ERROR("SingleNodeWorker: failed to register query {} from etcd: {}",
                  queryId, registerResult.error().what());
        return;
    }

    auto startResult = startQuery(*registerResult);
    if (!startResult)
    {
        NES_ERROR("SingleNodeWorker: failed to start query {} from etcd: {}",
                  queryId, startResult.error().what());
    }
    else
    {
        NES_INFO("SingleNodeWorker: started query {} from etcd", queryId);
    }
}
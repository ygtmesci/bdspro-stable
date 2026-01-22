#include <SingleNodeWorkerConfiguration.hpp>

std::vector<NES::BaseOption*>
NES::SingleNodeWorkerConfiguration::getOptions()
{
    return {
        &workerConfiguration,
        &grpcAddressUri,
        &connection,
        &enableGoogleEventTrace,
        &queryPlanStoreDir,
        &enableEtcdReconciler,
        &etcdEndpoints,
        &etcdKeyPrefix,
        &etcdPollIntervalMs
    };
}
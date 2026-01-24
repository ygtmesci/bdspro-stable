#include <Reconciler/EtcdWorkerReconciler.hpp>
#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <utility>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>
#include <etcd/SyncClient.hpp>

namespace NES {

namespace { constexpr const char* ASSIGNMENTS_SEGMENT = "/assignments/"; }

std::string EtcdWorkerReconciler::encodeWorkerAddr(const std::string& addr) {
    std::string encoded = addr;
    std::replace(encoded.begin(), encoded.end(), ':', '_');
    return encoded;
}

EtcdWorkerReconciler::EtcdWorkerReconciler(
    EtcdReconcilerConfig config,
    std::string workerGrpcAddr,
    QueryDiscoveredCallback onQueryDiscovered)
    : config(std::move(config)), workerGrpcAddr(std::move(workerGrpcAddr)),
      encodedWorkerAddr(encodeWorkerAddr(this->workerGrpcAddr)),
      onQueryDiscovered(std::move(onQueryDiscovered)),
      client(std::make_unique<etcd::SyncClient>(this->config.endpoints))
{
}

EtcdWorkerReconciler::~EtcdWorkerReconciler() { stop(); }

void EtcdWorkerReconciler::start() {
    pollThread = Thread("etcd-reconciler", [this](const std::stop_token& token) { pollLoop(token); });
}

void EtcdWorkerReconciler::stop() {}

void EtcdWorkerReconciler::markQueryAsKnown(const std::string& distributedQueryId) {
    std::lock_guard lock(knownQueriesMutex);
    knownDistributedQueries.insert(distributedQueryId);
}

void EtcdWorkerReconciler::removeQueryAssignment(const std::string& distributedQueryId) {
    std::string key = config.keyPrefix + distributedQueryId + ASSIGNMENTS_SEGMENT + encodedWorkerAddr;
    NES_INFO("EtcdWorkerReconciler: removing assignment from etcd: {}", key);
    client->rm(key); // Standard etcd delete
}

bool EtcdWorkerReconciler::isConnected() const {
    etcd::Response response = client->get("/__health_check__");
    return response.error_code() == 0 || response.error_code() == 100;
}

void EtcdWorkerReconciler::pollLoop(const std::stop_token& token) {
    NES::Thread::initializeThread(WorkerId("etcd-reconciler"), "etcd-reconciler");
    while (!token.stop_requested()) {
        try { reconcile(); } catch (...) {}
        std::this_thread::sleep_for(config.pollInterval);
    }
}

void EtcdWorkerReconciler::reconcile() {
    etcd::Response response = client->ls(config.keyPrefix);
    if (!response.is_ok()) return;

    const std::string workerSegment = std::string(ASSIGNMENTS_SEGMENT) + encodedWorkerAddr;
    for (const auto& key : response.keys()) {
        if (key.find(workerSegment) == std::string::npos) continue;
        std::string remainder = key.substr(config.keyPrefix.size());
        auto slashPos = remainder.find('/');
        if (slashPos == std::string::npos) continue;
        std::string distributedQueryId = remainder.substr(0, slashPos);

        {
            std::lock_guard lock(knownQueriesMutex);
            if (knownDistributedQueries.contains(distributedQueryId)) continue;
            knownDistributedQueries.insert(distributedQueryId);
        }

        etcd::Response getResponse = client->get(key);
        if (!getResponse.is_ok()) continue;
        SerializableQueryPlan proto;
        proto.ParseFromString(getResponse.value().as_string());
        LogicalPlan plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
        try { onQueryDiscovered(distributedQueryId, std::move(plan)); } catch (...) {}
    }
}
}
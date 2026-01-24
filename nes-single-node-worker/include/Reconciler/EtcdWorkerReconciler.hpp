#pragma once
#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <Identifiers/Identifiers.hpp>
#include <Plans/LogicalPlan.hpp>
#include <Thread.hpp>

namespace etcd { class SyncClient; }

namespace NES {

struct EtcdReconcilerConfig {
    std::string endpoints = "http://etcd:2379";
    std::string keyPrefix = "/nes/queries/";
    std::chrono::milliseconds pollInterval{1000};
};

using QueryDiscoveredCallback = std::function<void(const std::string& distributedQueryId, LogicalPlan)>;

class EtcdWorkerReconciler {
public:
    EtcdWorkerReconciler(
        EtcdReconcilerConfig config,
        std::string workerGrpcAddr,
        QueryDiscoveredCallback onQueryDiscovered);
    ~EtcdWorkerReconciler();

    void start();
    void stop();
    void markQueryAsKnown(const std::string& distributedQueryId);
    void removeQueryAssignment(const std::string& distributedQueryId); // NEW
    [[nodiscard]] bool isConnected() const;

private:
    void pollLoop(const std::stop_token& token);
    void reconcile();
    static std::string encodeWorkerAddr(const std::string& addr);

    EtcdReconcilerConfig config;
    std::string workerGrpcAddr;
    std::string encodedWorkerAddr;
    QueryDiscoveredCallback onQueryDiscovered;
    std::unique_ptr<etcd::SyncClient> client;
    Thread pollThread;
    std::unordered_set<std::string> knownDistributedQueries;
    std::mutex knownQueriesMutex;
};
}
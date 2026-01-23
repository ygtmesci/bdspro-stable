/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

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

namespace etcd {
class SyncClient;
}

namespace NES {

struct EtcdReconcilerConfig {
    std::string endpoints = "http://etcd:2379";
    std::string keyPrefix = "/nes/queries/";
    std::chrono::milliseconds pollInterval{1000};
};

/// Callback type for when a new query assignment is discovered.
/// Takes the distributed query ID (horse name) and the logical plan.
using QueryDiscoveredCallback = std::function<void(const std::string& distributedQueryId, LogicalPlan)>;

/// Polls etcd for query assignments and invokes callback for new queries.
/// Tracks already-processed queries to avoid duplicate execution.
class EtcdWorkerReconciler {
public:
    EtcdWorkerReconciler(
        EtcdReconcilerConfig config,
        std::string workerGrpcAddr,
        QueryDiscoveredCallback onQueryDiscovered);
    
    ~EtcdWorkerReconciler();

    /// Start the background polling thread
    void start();

    /// Stop polling (called automatically on destruction)
    void stop();

    /// Mark a distributed query as known (e.g., from local recovery)
    void markQueryAsKnown(const std::string& distributedQueryId);

    /// Check if connected to etcd
    [[nodiscard]] bool isConnected() const;

private:
    void pollLoop(const std::stop_token& token);
    void reconcile();

    /// Encode/decode worker address for etcd keys
    static std::string encodeWorkerAddr(const std::string& addr);

    EtcdReconcilerConfig config;
    std::string workerGrpcAddr;
    std::string encodedWorkerAddr;
    QueryDiscoveredCallback onQueryDiscovered;

    std::unique_ptr<etcd::SyncClient> client;
    Thread pollThread;

    /// Track distributed query IDs we've already seen/processed (horse names like "bold_appaloosa")
    std::unordered_set<std::string> knownDistributedQueries;
    std::mutex knownQueriesMutex;
};

} // namespace NES
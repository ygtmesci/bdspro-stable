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

#include <Reconciler/EtcdWorkerReconciler.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <thread>
#include <utility>

#include <Serialization/QueryPlanSerializationUtil.hpp>
#include <SerializableQueryPlan.pb.h>
#include <Util/Logger/Logger.hpp>

#include <etcd/SyncClient.hpp>

namespace NES {

namespace {
constexpr const char* ASSIGNMENTS_SEGMENT = "/assignments/";
}

std::string EtcdWorkerReconciler::encodeWorkerAddr(const std::string& addr) {
    std::string encoded = addr;
    std::replace(encoded.begin(), encoded.end(), ':', '_');
    return encoded;
}

EtcdWorkerReconciler::EtcdWorkerReconciler(
    EtcdReconcilerConfig config,
    std::string workerGrpcAddr,
    QueryDiscoveredCallback onQueryDiscovered)
    : config(std::move(config))
    , workerGrpcAddr(std::move(workerGrpcAddr))
    , encodedWorkerAddr(encodeWorkerAddr(this->workerGrpcAddr))
    , onQueryDiscovered(std::move(onQueryDiscovered))
    , client(std::make_unique<etcd::SyncClient>(this->config.endpoints))
{
    NES_INFO("EtcdWorkerReconciler: connecting to {} for worker {} (encoded: {})",
             this->config.endpoints, this->workerGrpcAddr, encodedWorkerAddr);
}

EtcdWorkerReconciler::~EtcdWorkerReconciler() {
    stop();
}

void EtcdWorkerReconciler::start() {
    NES_INFO("EtcdWorkerReconciler: starting poll loop (interval: {}ms)",
             config.pollInterval.count());
    
    pollThread = Thread("etcd-reconciler", [this](const std::stop_token& token) {
        pollLoop(token);
    });
}

void EtcdWorkerReconciler::stop() {
    // Thread destructor handles stop request and join
}

void EtcdWorkerReconciler::markQueryAsKnown(const std::string& distributedQueryId) {
    std::lock_guard lock(knownQueriesMutex);
    knownDistributedQueries.insert(distributedQueryId);
    NES_DEBUG("EtcdWorkerReconciler: marked distributed query {} as known", distributedQueryId);
}

bool EtcdWorkerReconciler::isConnected() const {
    etcd::Response response = client->get("/__health_check__");
    return response.error_code() == 0 || response.error_code() == 100;
}

void EtcdWorkerReconciler::pollLoop(const std::stop_token& token) {
    // Initialize thread context for NES
    NES::Thread::initializeThread(WorkerId("etcd-reconciler"), "etcd-reconciler");
    
    while (!token.stop_requested()) {
        try {
            reconcile();
        } catch (const std::exception& e) {
            NES_ERROR("EtcdWorkerReconciler: reconcile failed: {}", e.what());
        }
        
        std::this_thread::sleep_for(config.pollInterval);
    }
    NES_INFO("EtcdWorkerReconciler: poll loop stopped");
}

void EtcdWorkerReconciler::reconcile() {
    // List all keys under the query prefix
    etcd::Response response = client->ls(config.keyPrefix);
    
    if (!response.is_ok()) {
        if (response.error_code() == 100) {
            // Key not found - no queries yet
            return;
        }
        NES_WARNING("EtcdWorkerReconciler: ls failed: {}", response.error_message());
        return;
    }

    const std::string workerSegment = std::string(ASSIGNMENTS_SEGMENT) + encodedWorkerAddr;

    for (const auto& key : response.keys()) {
        // Check if this key is assigned to us
        if (key.find(workerSegment) == std::string::npos) {
            continue;
        }

        // Extract distributed query ID from key: {prefix}{distributedQueryId}/assignments/{worker}
        std::string remainder = key.substr(config.keyPrefix.size());
        auto slashPos = remainder.find('/');
        if (slashPos == std::string::npos) {
            continue;
        }
        
        // This is the horse name like "bold_appaloosa", NOT a UUID
        std::string distributedQueryId = remainder.substr(0, slashPos);

        // Check if we already know this distributed query
        {
            std::lock_guard lock(knownQueriesMutex);
            if (knownDistributedQueries.contains(distributedQueryId)) {
                continue;
            }
        }

        NES_INFO("EtcdWorkerReconciler: discovered new query assignment: {}", distributedQueryId);

        // Fetch the plan
        etcd::Response getResponse = client->get(key);
        if (!getResponse.is_ok()) {
            NES_WARNING("EtcdWorkerReconciler: failed to get plan for {}", key);
            continue;
        }

        // Deserialize
        SerializableQueryPlan proto;
        if (!proto.ParseFromString(getResponse.value().as_string())) {
            NES_WARNING("EtcdWorkerReconciler: failed to parse plan from {}", key);
            continue;
        }

        LogicalPlan plan = QueryPlanSerializationUtil::deserializeQueryPlan(proto);
        NES_INFO("Deserialized plan: {}", plan);  // or explain(plan, ExplainVerbosity::Debug)
        NES_INFO("Root operators count: {}", plan.getRootOperators().size());

        // Mark as known before callback to prevent duplicate processing
        {
            std::lock_guard lock(knownQueriesMutex);
            knownDistributedQueries.insert(distributedQueryId);
        }

        // Invoke callback with the distributed query ID (horse name)
        try {
            onQueryDiscovered(distributedQueryId, std::move(plan));
        } catch (const std::exception& e) {
            NES_ERROR("EtcdWorkerReconciler: callback failed for {}: {}", distributedQueryId, e.what());
        }
    }
}

} // namespace NES
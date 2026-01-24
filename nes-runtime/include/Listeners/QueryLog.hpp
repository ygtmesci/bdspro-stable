#pragma once

#include <chrono>
#include <optional>
#include <ostream>
#include <unordered_map>
#include <vector>
#include <functional> // Added
#include <Identifiers/Identifiers.hpp>
#include <Listeners/AbstractQueryStatusListener.hpp>
#include <Runtime/Execution/QueryStatus.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <folly/Synchronized.h>
#include <ErrorHandling.hpp>

namespace NES
{

struct QueryMetrics
{
    std::optional<std::chrono::system_clock::time_point> start;
    std::optional<std::chrono::system_clock::time_point> running;
    std::optional<std::chrono::system_clock::time_point> stop;
    std::optional<Exception> error;
};

struct LocalQueryStatus
{
    LocalQueryId queryId = INVALID_LOCAL_QUERY_ID;
    QueryState state = QueryState::Registered;
    QueryMetrics metrics{};
};

struct QueryStateChange
{
    QueryStateChange(const QueryState state, const std::chrono::system_clock::time_point timestamp) : state(state), timestamp(timestamp) { }
    QueryStateChange(Exception exception, std::chrono::system_clock::time_point timestamp);
    friend std::ostream& operator<<(std::ostream& os, const QueryStateChange& statusChange);
    QueryState state;
    std::chrono::system_clock::time_point timestamp;
    std::optional<Exception> exception;
};

struct QueryLog : AbstractQueryStatusListener
{
    using Log = std::vector<QueryStateChange>;
    using QueryStatusLog = std::unordered_map<LocalQueryId, std::vector<QueryStateChange>>;

    bool logSourceTermination(
        LocalQueryId queryId, OriginId sourceId, QueryTerminationType, std::chrono::system_clock::time_point timestamp) override;
    bool logQueryFailure(LocalQueryId queryId, Exception exception, std::chrono::system_clock::time_point timestamp) override;
    bool logQueryStatusChange(LocalQueryId queryId, QueryState status, std::chrono::system_clock::time_point timestamp) override;

    [[nodiscard]] std::optional<Log> getLogForQuery(LocalQueryId queryId) const;
    [[nodiscard]] std::optional<LocalQueryStatus> getQueryStatus(LocalQueryId queryId) const;
    [[nodiscard]] std::vector<LocalQueryStatus> getStatus() const;

    // NEW: Mechanism to notify reconciler when a query is done
    using OnCompletionCallback = std::function<void(const std::string& distributedQueryId)>;
    void setOnCompletionCallback(OnCompletionCallback callback) { completionCallback = std::move(callback); }
    void addDistributedMapping(LocalQueryId localId, std::string distId) {
        auto map = idMapping.wlock();
        (*map)[localId] = std::move(distId);
    }

private:
    folly::Synchronized<QueryStatusLog> queryStatusLog;
    // NEW: Internal state for deletion tracking
    folly::Synchronized<std::unordered_map<LocalQueryId, std::string>> idMapping;
    OnCompletionCallback completionCallback;
};
}
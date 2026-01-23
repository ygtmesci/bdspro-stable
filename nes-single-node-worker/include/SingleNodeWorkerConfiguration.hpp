#pragma once

#include <string>
#include <Configuration/WorkerConfiguration.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ScalarOption.hpp>
#include <Util/URI.hpp>

namespace NES {

class SingleNodeWorkerConfiguration final : public BaseConfiguration {
public:
    /// Connection name ({Hostname}:{PORT}) - NO DEFAULT VALUE
    ScalarOption<NES::URI> connection{
        "connection",
        "Connection name. This is the {Hostname}:{PORT}"
    };

    /// GRPC Server Address URI
    ScalarOption<NES::URI> grpcAddressUri{
        "grpc",
        "localhost:8080",
        R"(The address to try to bind to the server in URI form. If
the scheme name is omitted, "dns:///" is assumed. To bind to any address,
please use IPv6 any, i.e., [::]:<port>, which also accepts IPv4
connections. Valid values include dns:///localhost:1234,
192.168.1.1:31416, dns:///[::1]:27182, etc.)"
    };

    /// Enable Google Event Trace logging
    BoolOption enableGoogleEventTrace{
        "enable_event_trace",
        "false",
        "Enable Google Event Trace logging that generates Chrome tracing compatible JSON files."
    };

    /// Directory for persisting query plans
    ScalarOption<std::string> queryPlanStoreDir{
        "queryPlanStoreDir",
        "",
        "Directory to persist serialized logical query plans for fault tolerance"
    };

    /// Enable etcd-based query reconciliation
    BoolOption enableEtcdReconciler{
        "enableEtcdReconciler",
        "false",
        "Enable polling etcd for query assignments"
    };

    /// etcd endpoints for reconciler
    ScalarOption<std::string> etcdEndpoints{
        "etcdEndpoints",
        "http://etcd:2379",
        "etcd endpoint(s) for query assignment discovery"
    };

    /// etcd key prefix
    ScalarOption<std::string> etcdKeyPrefix{
        "etcdKeyPrefix",
        "/nes/queries/",
        "etcd key prefix for query assignments"
    };

    /// Reconciler poll interval in milliseconds
    ScalarOption<std::string> etcdPollIntervalMs{
        "etcdPollIntervalMs",
        "1000",
        "Interval in milliseconds between etcd polls"
    };

protected:
    std::vector<BaseOption*> getOptions() override;

    template <typename T>
    friend void generateHelp(std::ostream& ostream);

public:
    SingleNodeWorkerConfiguration() = default;

    /// NodeEngine configuration subtree
    WorkerConfiguration workerConfiguration{
        "worker",
        "NodeEngine Configuration"
    };
};

} // namespace NES
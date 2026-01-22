add_source_files(nes-single-node-worker
        SingleNodeWorker.cpp
        SingleNodeWorkerConfiguration.cpp
        GrpcService.cpp
        GoogleEventTracePrinter.cpp
        CompositeStatisticListener.cpp

        # Worker state persistence
        WorkerState/FileWorkerQueryPlanStore.cpp

        # etcd Reconciler
        Reconciler/EtcdWorkerReconciler.cpp
)
package com.aerospike;

public enum WorkloadStatus {
    Initialized,
    CanRun,
    PendingRun,
    Running,
    Completed,
    PendingShutdown,
    Shutdown
}


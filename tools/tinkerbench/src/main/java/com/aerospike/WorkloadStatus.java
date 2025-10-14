package com.aerospike;

public enum WorkloadStatus {
    Initialized,
    CanRun,
    PendingRun,
    Running,
    WaitingCompletion,
    Completed,
    PendingShutdown,
    Shutdown
}


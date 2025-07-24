package com.aerospike;

import java.util.concurrent.Callable;

/*
Query workloads should inherit from this interface.
The call function should be the actual workload that will be performed and measured.
If the call function should return false or throws an InterruptedException; this indicates not to measure this workload and the call is treated as aborted.
If an exception occurs, it is captured and treated as an error which will be included in the call-per-sec rate calculations (aborted calls do not).
 */
public interface QueryRunnable extends AGSGraphTraversal, Callable<Boolean> {

    /**
     * @return thw Query name
     */
    String Name();

    /*
    Returns true if this workload is a warmup...
     */
    boolean isWarmup();

    WorkloadTypes WorkloadType();

    QueryRunnable Start();

    QueryRunnable awaitTermination();

    QueryRunnable Shutdown();

    QueryRunnable PrintSummary();

    /*
    Returns the description of the query.
     */
    String getDescription();

    /*
    Performs a "Warm Up" or preprocessing prior to calling "run".
    Note: This is not executed within the scheduler.
    Returns:
        True to perform normal processing. False to cancel execution.
     */
    boolean preProcess() throws InterruptedException;

    /*
    Preforms post-processing after calling "run".
    Note: This is not executed within the scheduler.
     */
    void postProcess();
}

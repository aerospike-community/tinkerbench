package com.aerospike;

import org.javatuples.Pair;

import java.util.concurrent.Callable;

/*
Query workloads should inherit from this interface.
The call function should be the actual workload that will be performed and measured.
If the call function should return a pair where the key (first element) will be a boolean and the value (second element) is an object passed to the postCall function.
    If the first element is false, the workload is NOT measured. If true, the workload is measured.
    If the call function throws an InterruptedException; the workload is, also.
    If the call is not measured; the workload is treated as aborted.
    If an exception occurs, it is captured and treated as an error which will be included in the call-per-sec rate calculations (aborted calls do not).
 */
public interface QueryRunnable extends AGSGraphTraversal,
                                            Callable<Pair<Boolean,Object>> {

    /**
     * @return thw Query name
     */
    String Name();

    /*
    Returns true if this workload is a warmup...
     */
    boolean isWarmup();

    /*
    If true, this signals the result of the query should be displayed/logged
     */
    boolean isPrintResult();

    /*
    Prints the result from a Query
     */
    <V> void PrintResult(V result);

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

    /*
    Called before the actual workload is executed.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    void preCall();

    /*
    Called after the actual workload is executed passing the value type T from the workload.
        success is true if the workload was recorded and exception will be not-null if an exciton occurred.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    void postCall(Object value, Boolean success, Throwable exception);
}

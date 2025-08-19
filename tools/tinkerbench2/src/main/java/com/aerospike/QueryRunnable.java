package com.aerospike;

import org.javatuples.Pair;

import java.util.concurrent.Callable;

/*
Query workloads should inherit from this interface.
The call function should be the actual workload that will be performed and measured.
If the call function should return a pair where:
        1) the key (first element) will be a boolean
        2) the value (second element) is an object passed to the postCall function
The call function additional notes:
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
    Returns a label used to obtain the ids in the sampling.
        If null, the default from the CLI is used.
    Note: If a label is provided in the CLI, that value will override this value.
     */
    String getSampleLabelId();

    /*
    Returns the sampling size.
        If -1 to use the default value from the CLI.
        If zero, this will disable id sampling.
     */
    int getSampleSize();

    /*
    Returns a vertex Id from the IDManger or null
     */
    Object getVId();

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
    Called after the actual workload is executed passing the value from the workload.
        The success param is true if the workload was recorded and exception will be not-null if an exciton occurred during workload execution.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    void postCall(Object value, Boolean success, Throwable exception);
}

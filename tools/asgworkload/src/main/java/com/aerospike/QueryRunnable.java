package com.aerospike;

import java.util.concurrent.Callable;

/*
Query workloads should inherit from this interface.
The call function should be the actual workload that will be performed and measured.
If the call function should return false or throws an InterruptedException; this indicates not to measure this workload and the call is treated as aborted.
If an exception occurs, it is captured and treated as an error which will be included in the call-per-sec rate calculations (aborted calls do not).
 */
public interface QueryRunnable extends Callable<Boolean> {

    public void setWorkloadProvider(WorkloadProvider provider);

    public String Name();

    public WorkloadTypes WorkloadType();

    /*
    Returns the description of the query.
     */
    public String getDescription();

    /*
    Performs a "Warm Up" or preprocessing prior to calling "run".
    Note: This is not executed within the scheduler.
    Returns:
        True to perform normal processing. False to cancel execution.
     */
    public boolean preProcess() throws InterruptedException;

    /*
    Preforms post-processing after calling "run".
    Note: This is not executed within the scheduler.
     */
    public void postProcess();
}

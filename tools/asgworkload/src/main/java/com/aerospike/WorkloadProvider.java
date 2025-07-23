package com.aerospike;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface WorkloadProvider extends AutoCloseable {

    /*
    Returns true if current run is a warmup.
     */
    public boolean isWarmup();

    /*
    The targeted calls-per-second.
     */
    public int getTargetCallsPerSecond();

    /*
    The target duration of executing the workload.
     */
    public Duration getTargetRunDuration();

    /*
    The running execution duration.
     */
    public Duration getExecutingDuration();

    /*
    The number of pending calls.
     */
    public int getPendingCount();

    /*
    Number of calls aborted
     */
    public int getAbortedCount();

    /*
    The number of success calls.
     */
    public int getSuccessCount();

    /*
    The number of errors encountered.
     */
    public int getErrorCount();

    /*
    The collection of errors encountered.
     */
    public List<Exception> getErrors();

    /*
    Returns the Status of the workload scheduler.
     */
    public WorkloadStatus getStatus();

    /*
    The start time of execution.
     */
    public LocalDateTime getStartDateTime();

    /*
    Returns the current Success calls-per-second rate.
     */
    public double getCallsPerSecond();

    /*
    Returns the current errors-per-second rate.
     */
    public double getErrorsPerSecond();

    public OpenTelemetry getOpenTelemetry();

    public AGSWorkloadArgs getCliArgs();

    /*
    Set's the query that will be executed by the work load scheduler.
    If the value is null or changed, the scheduler is closed and reset.
     */
    public WorkloadProvider setQuery(QueryRunnable queryRunnable);

    /*
    Executes the Query, if one is defined.
    If the query was already ran (completed), it will be re-executed. If the workload was shutdown, a RuntimeException is thrown.
     */
    public WorkloadProvider Start() throws RuntimeException;

    /*
    Shuts-down the workload scheduler. If the workload is running, this will wait until completion.
    If the scheduler is already shutdown, it just returns.
     */
    public WorkloadProvider Shutdown();

    /*
   Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
   Params:
       timeout – the maximum time to wait unit before terminating workload.
                   If -1, the default duration is used.
       time unit - The unit of time
   Returns:
       true if this executor terminated and false if the timeout elapsed before termination or nothing is running.
   Throws:
       InterruptedException – if interrupted while waiting
    */
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException;

    /*
    Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
    Returns:
        true if this executor terminated and false if the timeout elapsed before termination or nothing is running.
     */
    public boolean awaitTermination();

    public WorkloadProvider PrintSummary();
}

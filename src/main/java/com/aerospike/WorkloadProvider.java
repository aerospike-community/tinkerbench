package com.aerospike;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

public interface WorkloadProvider extends AutoCloseable {

    /*
    Returns true if current run is a warmup.
     */
    boolean isWarmup();

    boolean isAborted();

    boolean isDebug();

    /*
    The targeted calls-per-second.
     */
    int getTargetCallsPerSecond();

    /*
    The target duration of executing the workload.
     */
    Duration getTargetRunDuration();

    /*
    The running execution duration.
     */
    Duration getAccumDuration();

    /*
    The number of pending calls.
     */
    long getPendingCount();

    /*
    Number of calls aborted
     */
    long getAbortedCount();

    /*
    The amount of time accumulative taken for successful executions (not wall clock)
     */
    Duration getAccumSuccessDuration();

    /*
    The number of success calls.
     */
    long getSuccessCount();

    /*
    The number of errors encountered.
     */
    long getErrorCount();

    /*
    The collection of errors encountered.
     */
    List<Exception> getErrors();

    /*
   The amount of time accumulative taken for error execution state (not wall clock)
    */
    Duration getAccumErrorDuration();

    /*
    Returns the Status of the workload scheduler.
     */
    WorkloadStatus getStatus();

    /*
    The start time of execution.
     */
    LocalDateTime getStartDateTime();

    /*
    The completion time
     */
    LocalDateTime getCompletionDateTime();

    /*
    Returns the remaining time of the run based on the Targeted Run Duration.
     */
    Duration getRemainingTime();

    /*
    Returns the current Success calls-per-second rate.
     */
    double getCallsPerSecond();

    /*
    Returns the difference as a Percentage between actual and target CPS
     */
    double getCPSDiffPct();

    /*
    Returns the current errors-per-second rate.
     */
    double getErrorsPerSecond();

    OpenTelemetry getOpenTelemetry();

    TinkerBenchArgs getCliArgs();

    /*
    Set's the query that will be executed by the work load scheduler.
    If the value is null or changed, the scheduler is closed and reset.
     */
    WorkloadProvider setQuery(QueryRunnable queryRunnable);

    /*
    Executes the Query, if one is defined.
    If the query was already ran (completed), it will be re-executed. If the workload was shutdown, a RuntimeException is thrown.
     */
    WorkloadProvider Start() throws RuntimeException;

    /*
    Shuts-down the workload scheduler. If the workload is running, this will wait until completion.
    If the scheduler is already shutdown, it just returns.
     */
    WorkloadProvider Shutdown();

    /*
    Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
    Returns:
        true if this executor terminated and false if the timeout elapsed before termination or nothing is running.
     */
    boolean awaitTermination();

    WorkloadProvider PrintSummary();

    /*
    Signals that workload should be aborted.
     */
    void SignalAbortWorkLoad();
    void SignalAbortWorkLoadPrintResults() throws InterruptedException;

    long AddError(Exception e);
}

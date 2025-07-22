package com.aerospike;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WorkloadProviderScheduler implements WorkloadProvider {

    private WorkloadStatus status;

    private final int schedulers;
    private final ExecutorService schedulerPool;
    private final ExecutorService workerPool;
    private final Duration targetRunDuration;
    private final Duration shutdownTimeout;
    private final AtomicInteger pendingCount = new AtomicInteger();
    private final AtomicInteger abortedCount = new AtomicInteger();
    private final AtomicInteger successCount = new AtomicInteger();
    private final AtomicInteger errorCount = new AtomicInteger();
    private final AtomicLong totCallDuration = new AtomicLong();
    private final Vector<Exception> errors = new Vector<>();
    private final int callsPerSecond;
    private final OpenTelemetry openTelemetry;
    private final AGSWorkloadArgs cliArgs;

    private QueryRunnable queryRunnable = null;
    private long startTimerNS;
    private long endTimerNS;
    private LocalDateTime startDateTime = null;
    private LocalDateTime stopDateTime = null;

    private void setStatus(WorkloadStatus workloadStatus) {

        if(this.status != workloadStatus) {
            this.status = workloadStatus;

            if (workloadStatus == WorkloadStatus.Running && startDateTime == null)
                startDateTime = LocalDateTime.now();
            else if (startDateTime != null
                        && stopDateTime == null
                        && (workloadStatus == WorkloadStatus.Completed
                                || workloadStatus == WorkloadStatus.PendingShutdown
                                || workloadStatus == WorkloadStatus.Shutdown))
                stopDateTime = LocalDateTime.now();

            if (openTelemetry != null)
                openTelemetry.setConnectionState(workloadStatus.toString());
        }
    }

    /*
    The number of defined Schedulers.
     */
    public int getSchedulers() { return schedulers; }

    /*
    The targeted calls-per-second.
     */
    public int getTargetCallsPerSecond() { return callsPerSecond; }
    /*
    The target duration of executing the workload.
     */
    public Duration getTargetRunDuration() { return targetRunDuration; }
    /*
    The running execution duration.
     */
    public Duration getExecutingDuration() { return Duration.ofNanos(totCallDuration.get()); }
    /*
    The number of pending calls.
     */
    public int getPendingCount() { return pendingCount.get(); }
    /*
    Number of calls aborted
     */
    public int getAbortedCount() { return abortedCount.get(); }
    /*
    The number of success calls.
     */
    public int getSuccessCount() { return successCount.get();}

    /*
    The number of errors encountered.
     */
    public int getErrorCount() { return errorCount.get(); }

    /*
    The collection of errors encountered.
     */
    public List<Exception> getErrors() { return errors.stream().toList(); }

    /*
    Returns the Status of the workload scheduler.
     */
    public WorkloadStatus getStatus() { return status; }

    /*
    The start time of execution.
     */
    public LocalDateTime getStartDateTime() { return startDateTime; }

    /*
    The completion time
     */
    public LocalDateTime getCompletionDateTime() { return stopDateTime; }

    /*
    Returns the current calls-per-second.
     */
    public double getCallsPerSecond() {
        return totCallDuration.get() == 0L ? 0.0 : (successCount.doubleValue() + errorCount.doubleValue()) / (totCallDuration.doubleValue() / 1_000_000_000.0);
    }

    public OpenTelemetry getOpenTelemetry() { return openTelemetry; }

    public AGSWorkloadArgs getCliArgs() { return cliArgs; }

    public WorkloadProviderScheduler(int schedulers,
                                     int workers,
                                     Duration duration,
                                     int callsPerSecond,
                                     Duration shutdownTimeout,
                                     OpenTelemetry openTelemetry,
                                     AGSWorkloadArgs cliArgs) {

        this.targetRunDuration = duration;
        this.callsPerSecond = callsPerSecond;
        this.shutdownTimeout = shutdownTimeout;
        this.schedulers = schedulers;
        this.openTelemetry = openTelemetry == null ? new OpenTelemetryDummy() : openTelemetry;
        this.cliArgs = cliArgs;

        schedulerPool = Executors.newFixedThreadPool(schedulers);
        workerPool = Executors.newFixedThreadPool(workers);

        setStatus(WorkloadStatus.Initialized);
    }

    public WorkloadProviderScheduler(int schedulers,
                                     int workers,
                                     Duration duration,
                                     int callsPerSecond,
                                     Duration shutdownTimeout,
                                     OpenTelemetry openTelemetry,
                                     AGSWorkloadArgs cliArgs,
                                     QueryRunnable query) {
        this(schedulers,
                workers,
                duration,
                callsPerSecond,
                shutdownTimeout,
                openTelemetry,
                cliArgs);
        this.setQuery(query);
    }

    public WorkloadProviderScheduler(int schedulers,
                                     int workers,
                                     Duration duration,
                                     int callsPerSecond,
                                     Duration shutdownTimeout,
                                     OpenTelemetry openTelemetry,
                                     AGSWorkloadArgs cliArgs,
                                     QueryRunnable query,
                                     boolean startRun) {
        this(schedulers,
                workers,
                duration,
                callsPerSecond,
                shutdownTimeout,
                openTelemetry,
                cliArgs,
                query);
        if(startRun)
            Start();
    }

    /*
    Set's the query that will be executed by the work load scheduler.
    If the value is null or changed, the scheduler is closed and reset.
     */
    public WorkloadProvider setQuery(QueryRunnable queryRunnable) {

        if(queryRunnable == null) {
            this.close();
            setStatus(WorkloadStatus.Initialized);
            this.queryRunnable = null;
        }
        else {
            this.queryRunnable = queryRunnable;
            queryRunnable.setWorkloadProvider(this);
            if(openTelemetry != null) {
                openTelemetry.setWorkloadName(queryRunnable.WorkloadType().toString(),
                                                queryRunnable.Name());
            }
            setStatus(WorkloadStatus.CanRun);
        }
        return this;
    }

    /*
    Executes the Query, if one is defined.
    If the query was already ran (completed), it will be re-executed. If the workload was shutdown, a RuntimeException is thrown.
     */
    public WorkloadProvider Start() throws RuntimeException {

        if(status == WorkloadStatus.CanRun || status == WorkloadStatus.Completed) {
            final int useScheduler = schedulers * 2;
            final int base = callsPerSecond / useScheduler;
            final int remainder = callsPerSecond % useScheduler;
            setStatus(WorkloadStatus.PendingRun);

            if(queryRunnable != null) {
                try {
                    if (!queryRunnable.preProcess()) {
                        setStatus(WorkloadStatus.Completed);
                        return this;
                    }
                }
                catch (Exception e) {
                    AddError(e);
                    return this;
                }
            }

            for (int i = 0; i < schedulers; i++) {
                final int rate = base + (i < remainder ? 1 : 0);
                schedulerPool.submit(() -> runDispatcher(rate));
            }
            return this;
        }
        if(status == WorkloadStatus.Running)
            return this;

        throw new RuntimeException("Cannot Start an Workload in state " + status);
    }

    /*
    Shuts-down the workload scheduler. If the workload is running, this will wait until completion.
    If the scheduler is already shutdown, it just returns.
     */
    public WorkloadProvider Shutdown() { close(); return this; }

    /**
     * Waits for the workload to termination and then performs a shutdown.
     */
    @Override
    public void close() {

        if(status == WorkloadStatus.Shutdown || status == WorkloadStatus.PendingShutdown)
            return;

        boolean alreadyCompleted = status == WorkloadStatus.Completed;

        setStatus(WorkloadStatus.PendingShutdown);
        shutdownAndAwaitTermination(schedulerPool, "Scheduler");
        shutdownAndAwaitTermination(workerPool, "Worker");
        setStatus(WorkloadStatus.Shutdown);

        if(!alreadyCompleted && queryRunnable != null) {
            queryRunnable.postProcess();
        }
        cliArgs.terminateRun.set(true);
    }

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
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {

        if(status == WorkloadStatus.Running) {
            if(timeout < 0) {
                timeout = targetRunDuration.toSeconds() + shutdownTimeout.toSeconds();
                unit = TimeUnit.SECONDS;
            }
            if(workerPool.awaitTermination(timeout,unit)) {
                if(!schedulerPool.isTerminated()) {
                    schedulerPool.awaitTermination(timeout, unit);
                }
                setStatus(WorkloadStatus.Completed);
                if(queryRunnable != null) {
                    queryRunnable.postProcess();
                }
                cliArgs.terminateRun.set(true);
                return true;
            }
        }

        return false;
    }

    /*
    Blocks until all tasks have completed execution after a shutdown request, or the timeout occurs, or the current thread is interrupted, whichever happens first.
    Returns:
        true if this executor terminated and false if the timeout elapsed before termination or nothing is running.
     */
    public boolean awaitTermination() {
        try {
            return awaitTermination(-1, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            return false;
        }
    }

    static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String toString() {

        if(totCallDuration.get() == 0)
        {
            return String.format("WorkloadProvider [status=%s, call/sec=, pending=%,d, successes=%,d, errors=%,d,aborted=%d, start=, stop=, run_duration=0, run_reminding=%s]",
                    status,
                                    pendingCount.get(),
                                    successCount.get(),
                                    errorCount.get(),
                                    abortedCount.get(),
                                    targetRunDuration);
        }

        Duration rtDuration = Duration.ofNanos(totCallDuration.get());
        Duration reminding = targetRunDuration.compareTo(rtDuration) > 0
                                ? targetRunDuration.minus(rtDuration)
                                : Duration.ZERO;

        return String.format("WorkloadProviderScheduler [status=%s, call/sec=%,.2f, pending=%,d, successes=%,d, errors=%,d, aborted=%d, start=%s, stop=%s, run_duration=%s, run_reminding=%s]",
                                status,
                                getCallsPerSecond(),
                                pendingCount.get(),
                                successCount.get(),
                                errorCount.get(),
                                abortedCount.get(),
                                startDateTime == null ? "N/A" : dtFormatter.format(startDateTime),
                                stopDateTime == null ? "N/A" : dtFormatter.format(stopDateTime),
                                rtDuration,
                                reminding);
    }

    class Handler implements Runnable {

        Handler() {  }

        public void run() {
            long startCall = 0;
            pendingCount.incrementAndGet();

            try {
                startCall = System.nanoTime();
                final boolean recordResult = queryRunnable.call();
                final long duration = System.nanoTime() - startCall;
                if(recordResult) {
                    totCallDuration.addAndGet(duration);
                    successCount.incrementAndGet();
                    if(openTelemetry != null) {
                        openTelemetry.incrTransCounter();
                        openTelemetry.recordElapsedTime(duration);
                    }
                }
                else abortedCount.incrementAndGet();
            } catch (InterruptedException e) {
                abortedCount.incrementAndGet();
            } catch (Exception e) {
                final long duration = System.nanoTime() - startCall;
                if(startCall > 0)
                    totCallDuration.addAndGet(duration);
                AddError(e);
            } finally {
                pendingCount.decrementAndGet();
            }
        }
    }

    private void runDispatcher(int rate) {

        final long callIntervalNS = 1_000_000_000L / rate;
        final long targetDurationNS = targetRunDuration.toNanos();
        long nextCallTime = System.nanoTime();
        List<Future<?>> futures = new ArrayList<>();
        setStatus(WorkloadStatus.Running);

        while (totCallDuration.get() <= targetDurationNS
                && !(cliArgs.abortRun.get()
                        && cliArgs.terminateRun.get())) {
            long now = System.nanoTime();
            if (now >= nextCallTime) {
                futures.add(workerPool.submit(new Handler()));
                nextCallTime += callIntervalNS;
            } else {
                Thread.onSpinWait();
            }
        }

        if(totCallDuration.get() >= targetDurationNS
                || cliArgs.abortRun.get()) {
            for (Future<?> future : futures) {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            }
        }
    }

    private boolean shutdownAndAwaitTermination(ExecutorService pool, String poolName) {
        boolean errorOccurred = false;

        if(pool.isShutdown())
            return false;

        long totDurationSecs = targetRunDuration.toSeconds() + shutdownTimeout.toSeconds();
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(totDurationSecs, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(totDurationSecs, TimeUnit.SECONDS)) {
                    System.err.printf("%s Pool did not terminate%n", poolName);
                    errorOccurred = true;
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            AddError(e);
        }

        return errorOccurred;
    }

    private void AddError(Exception e) {
        errors.add(e);
        errorCount.incrementAndGet();
        if(openTelemetry != null)
            openTelemetry.addException(e);
    }
}

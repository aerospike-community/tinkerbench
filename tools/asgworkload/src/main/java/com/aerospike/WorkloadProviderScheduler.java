package com.aerospike;

import org.HdrHistogram.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public final class WorkloadProviderScheduler implements WorkloadProvider {

    private static final Logger log = LoggerFactory.getLogger(WorkloadProviderScheduler.class);
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
    private final AtomicLong successDuration = new AtomicLong();
    private final AtomicLong errorDuration = new AtomicLong();
    private final Boolean warmup;
    private final Histogram histogram;

    private final Vector<Exception> errors = new Vector<>();
    private final int callsPerSecond;
    private final OpenTelemetry openTelemetry;
    private final AGSWorkloadArgs cliArgs;

    private QueryRunnable queryRunnable = null;

    private LocalDateTime startDateTime = null;
    private LocalDateTime stopDateTime = null;

    private final LogSource logger = LogSource.getInstance();

    private final List<Future<?>> schedulerFutures = new Vector<>();

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

            logger.PrintDebug("WorkloadProviderScheduler", "New Status %s for %s",
                                workloadStatus,
                                queryRunnable);
        }
    }

    /*
    Returns true if current run is a warmup.
     */
    public boolean isWarmup() { return warmup; }

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
    Returns the current Success calls-per-second rate.
     */
    public double getCallsPerSecond() {
        return successDuration.get() == 0L ? 0.0 : successCount.doubleValue() / (successDuration.doubleValue() / 1_000_000_000.0);
    }

    /*
    Returns the current errors-per-second rate.
     */
    public double getErrorsPerSecond() {
        return errorDuration.get() == 0L ? 0.0 : errorCount.doubleValue() / (errorDuration.doubleValue() / 1_000_000_000.0);
    }

    public OpenTelemetry getOpenTelemetry() { return openTelemetry; }

    public AGSWorkloadArgs getCliArgs() { return cliArgs; }

    public WorkloadProviderScheduler(OpenTelemetry openTelemetry,
                                     Duration targetRunDuration,
                                     boolean warmup,
                                     AGSWorkloadArgs cliArgs) {

        this.targetRunDuration =  targetRunDuration == null
                                    ? cliArgs.duration
                                    : targetRunDuration;
        this.callsPerSecond = cliArgs.callsPerSecond;
        this.shutdownTimeout = cliArgs.shutdownTimeout;
        this.schedulers = cliArgs.schedulars;
        this.openTelemetry = openTelemetry == null ? new OpenTelemetryDummy() : openTelemetry;
        this.cliArgs = cliArgs;
        this.warmup = warmup;
        {
            // A Histogram covering the range from 1 nsec to target duration with 3 decimal point resolution:
            final Duration higestDuration = targetRunDuration.plusMinutes(1);
            this.histogram = new AtomicHistogram(higestDuration.toNanos(), 3);
        }

        schedulerPool = Executors.newFixedThreadPool(this.schedulers);
        workerPool = Executors.newFixedThreadPool(cliArgs.workers);

        setStatus(WorkloadStatus.Initialized);
    }

    public WorkloadProviderScheduler(OpenTelemetry openTelemetry,
                                     Duration targetRunDuration,
                                     boolean warmup,
                                     AGSWorkloadArgs cliArgs,
                                     QueryRunnable query) {
        this(openTelemetry,
                targetRunDuration,
                warmup,
                cliArgs);
        this.setQuery(query);
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
            if(openTelemetry != null) {
                openTelemetry.Reset(cliArgs,
                                    queryRunnable.Name(),
                                    queryRunnable.WorkloadType().toString(),
                                    targetRunDuration,
                                    warmup,
                                    null);
            }
            setStatus(WorkloadStatus.CanRun);
            logger.PrintDebug("WorkloadProviderScheduler", "Set Query to %s", queryRunnable);
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
                    System.out.printf("Running Pre-Process for %s %s...",
                                        queryRunnable.WorkloadType().toString(),
                                        queryRunnable.Name());
                    if (!queryRunnable.preProcess()) {
                        setStatus(WorkloadStatus.Completed);
                        System.out.println(" Aborted");
                        logger.PrintDebug("WorkloadProviderScheduler",
                                        "Pre-Process returned false and workload %s NOT executed",
                                            this.queryRunnable);
                        logger.getLogger4j().warn("Pre-Process returned false and workload {} NOT executed",
                                                    this.queryRunnable);
                        return this;
                    }
                }
                catch (Exception e) {
                    logger.Print("WorkloadProviderScheduler",
                                    true,
                                    "Exception on Start for Workload %s",
                                    queryRunnable);
                    logger.Print("WorkloadProviderScheduler", e);
                    return this;
                }
            }
            System.out.println(" Completed");
            schedulerFutures.clear();
            System.out.printf("Starting %s for %s %s\n",
                                warmup ? "Warmup" : "Workload",
                                queryRunnable.WorkloadType().toString(),
                                queryRunnable.Name());

            for (int i = 0; i < schedulers; i++) {
                final int rate = base + (i < remainder ? 1 : 0);
                schedulerFutures.add(schedulerPool.submit(() -> runDispatcher(rate)));
            }

            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException ignored) {}

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

        if(queryRunnable != null) {
            System.out.printf("Pending Shutdown for %s %s\n",
                    queryRunnable.WorkloadType().toString(),
                    queryRunnable.Name());
        }

        setStatus(WorkloadStatus.PendingShutdown);
        shutdownAndAwaitTermination(schedulerPool, "Scheduler");
        shutdownAndAwaitTermination(workerPool, "Worker");
        setStatus(WorkloadStatus.Shutdown);

        if(!alreadyCompleted && queryRunnable != null) {
            System.out.printf("Running Post-process for %s %s %s...",
                                warmup ? "Warmup" : "Workload",
                                queryRunnable.WorkloadType().toString(),
                                queryRunnable.Name());
            queryRunnable.postProcess();
            System.out.println(" Completed");
        }
        cliArgs.terminateRun.set(true);

        if(queryRunnable != null) {
            System.out.printf("Shutdown for %s %s %s Completed\n",
                    warmup ? "Warmup" : "Workload",
                    queryRunnable.WorkloadType().toString(),
                    queryRunnable.Name());
        }
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

            System.out.println("\tAWaiting for Completion...");

            if(workerPool.awaitTermination(timeout,unit)) {
                if(!schedulerPool.isTerminated()) {
                    schedulerPool.awaitTermination(timeout, unit);
                }
                setStatus(WorkloadStatus.Completed);
                if(queryRunnable != null) {
                    queryRunnable.postProcess();
                }
                cliArgs.terminateRun.set(true);
                System.out.println("\tRun Completed");
                return true;
            }
        }

        System.out.println("\tRun Completed (Timed out)");
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

    public WorkloadProvider PrintSummary(PrintStream printStream) {

        if(queryRunnable == null) {
            printStream.printf("No %s Summary available.\n",
                                warmup ? "Warmup" : "Workload");
            return this;
        }
        final Duration rtDuration = Duration.ofNanos(totCallDuration.get());
        final int successCount = getSuccessCount();
        final int errorCount = getErrorCount();
        final int abortedCount = getAbortedCount();
        final int totalCount = successCount + errorCount + abortedCount;

        printStream.printf("%s Summary for %s:\n",
                            warmup ? "Warmup " : "Workload",
                            queryRunnable.Name());
        if(getStatus() != WorkloadStatus.Running)
            printStream.printf("\tStatus: %s\n", getStatus());
        printStream.printf("\tExecution Time: %s\n", rtDuration);
        printStream.printf("\tMean Calls/Second: %,.2f\n", getCallsPerSecond());
        printStream.printf("\tMean Errors/Second: %,.2f\n", getErrorsPerSecond());
        printStream.printf("\tSuccesses: %,d\n", successCount);
        printStream.printf("\tErrors: %,d\n", errorCount);
        printStream.printf("\tAborted: %,d\n", abortedCount);
        printStream.printf("\tTotal: %,d\n", totalCount);

        if(errorCount > 0) {
            printStream.println("Errors:");

            Map<String, List<Exception>> errors = getErrors()
                    .stream()
                    .collect(Collectors.groupingBy(w -> w.getClass().getName()));
            for (Map.Entry<String, List<Exception>> entry : errors.entrySet()) {
                Map<String, List<Exception>> sameMsgs = entry.getValue()
                        .stream()
                        .collect(Collectors.groupingBy(Throwable::getMessage));
                if (entry.getValue().size() == sameMsgs.size()) {
                    printStream.printf("\tCnt: %d\tException: %s\t'%s'\n",
                            entry.getValue().size(),
                            entry.getKey(),
                            entry.getValue().getFirst().getMessage());
                } else {
                    printStream.printf("\tCnt: %d\tException: %s:\n",
                            entry.getValue().size(),
                            entry.getKey());
                    for (Map.Entry<String, List<Exception>> sameMsg : sameMsgs.entrySet()) {
                        printStream.printf("\t\tCnt: %d\tMsg: '%s'\n",
                                sameMsgs.size(),
                                sameMsg.getKey());
                    }
                }
            }
        }
        printStream.println();
        if(!warmup) {
            printStream.printf("Recorded latencies [in usec] for %s\n",
                    queryRunnable.Name());
            histogram.outputPercentileDistribution(printStream, 1000.0);
            printStream.println();
            printStream.println();
        }
        return this;
    }

    public WorkloadProvider PrintSummary() {

        if(logger.getLogger4j().isInfoEnabled()) {
            try (LogSource.Stream logStream = new LogSource.Stream(logger)) {
                PrintSummary(logStream.getPrintStream());
                logStream.info();
            }
        }
        return PrintSummary(System.out);
    }

    static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @Override
    public String toString() {

        if(totCallDuration.get() == 0)
        {
            return String.format("WorkloadProvider [type=%s, status=%s, call/sec=, pending=%,d, successes=%,d, errors=%,d,aborted=%d, start=, stop=, run_duration=0, run_reminding=%s]",
                    warmup ? "warmup" : "workload",
                    status,
                    pendingCount.get(),
                    successCount.get(),
                    errorCount.get(),
                    abortedCount.get(),
                    targetRunDuration);
        }

        final Duration rtDuration = Duration.ofNanos(totCallDuration.get());
        final Duration reminding = targetRunDuration.compareTo(rtDuration) > 0
                                ? targetRunDuration.minus(rtDuration)
                                : Duration.ZERO;

        return String.format("WorkloadProviderScheduler [type=%s, status=%s, call/sec=%,.2f, pending=%,d, successes=%,d, errors=%,d, aborted=%d, start=%s, stop=%s, run_duration=%s, run_reminding=%s]",
                                warmup ? "warmup" : "workload",
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

    private final class Handler implements Runnable {

        Handler() {  }

        private void Success(long latency) {
            totCallDuration.addAndGet(latency);
            successDuration.addAndGet(latency);
            histogram.recordValue(latency);
            successCount.incrementAndGet();
            if(openTelemetry != null) {
                openTelemetry.incrTransCounter();
                openTelemetry.recordElapsedTime(latency);
            }
        }

        private void Error(long latency, Throwable e) {
            errors.add((Exception) e);
            errorCount.incrementAndGet();
            if(latency > 0) {
                totCallDuration.addAndGet(latency);
                errorDuration.addAndGet(latency);
            }
            logger.error(String.format("Workload %s",queryRunnable.Name()), e);
            if(openTelemetry != null)
                openTelemetry.addException((Exception)e);
        }

        public void run() {
            long startCall = 0;
            pendingCount.incrementAndGet();

            try {
                startCall = System.nanoTime();
                final boolean recordResult = queryRunnable.call();
                final long duration = System.nanoTime() - startCall;
                if(recordResult) {
                   Success(duration);
                }
                else {
                    abortedCount.incrementAndGet();
                    logger.getLogger4j().warn("Workload {} aborted",
                                                queryRunnable);
                }
            } catch (InterruptedException e) {
                abortedCount.incrementAndGet();
            } catch (CompletionException e) {
                final long duration = System.nanoTime() - startCall;
                Error(duration, e.getCause());
            } catch (Exception e) {
                final long duration = System.nanoTime() - startCall;
                Error(duration, e);
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
            //double maxRate = getCallsPerSecond();
            //if(maxRate > maxCallRate)
            //    maxCallRate = maxRate;
        }

        if(totCallDuration.get() >= targetDurationNS
                || cliArgs.abortRun.get()) {
            System.out.printf("\tStopping Workload due to %s...\n",
                                cliArgs.abortRun.get() ? "Signal" : "Time Limit");
            logger.PrintDebug("WorkloadProviderScheduler",
                            "Aborting Workers %s",
                            queryRunnable);
            for (Future<?> future : futures) {
                if (!future.isDone()) {
                    future.cancel(true);
                }
            }
        }
        else {
            System.out.println("\tStopping Workload...");
        }
    }

    private void shutdownAndAwaitTermination(ExecutorService pool, String poolName) {

        logger.PrintDebug("WorkloadProviderScheduler",
                            "shutdownAndAwaitTermination for Pool %s %s",
                            poolName,
                            toString());

        if(pool.isShutdown())
            return;

        long totDurationSecs = targetRunDuration.toSeconds() + shutdownTimeout.toSeconds();
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(totDurationSecs, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(totDurationSecs, TimeUnit.SECONDS)) {
                    System.err.printf("%s Pool did not terminate%n", poolName);
                }
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.Print("WorkloadProviderScheduler",
                    true,
                    "Exception on shutdownAndAwaitTermination for Workload %s for Pool %s",
                    queryRunnable,
                    poolName);
            logger.Print("WorkloadProviderScheduler", e);
        }

        logger.PrintDebug("WorkloadProviderScheduler",
                "shutdownAndAwaitTermination for Pool %s Completed %s",
                poolName,
                toString());

    }
}

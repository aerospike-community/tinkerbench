package com.aerospike;

import org.HdrHistogram.*;

import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public final class WorkloadProviderScheduler implements WorkloadProvider {

    private static final Logger log = LoggerFactory.getLogger(WorkloadProviderScheduler.class);

    private final int schedulers;
    private final ExecutorService schedulerPool;
    private final ExecutorService workerPool;
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);

    private final Duration targetRunDuration;
    private final Duration shutdownTimeout;
    private final AtomicLong pendingCount = new AtomicLong();
    private final AtomicLong abortedCount = new AtomicLong();
    private final AtomicLong successfulDuration = new AtomicLong();
    private final AtomicLong errorDuration = new AtomicLong();
    private final AtomicLong errorCount = new AtomicLong();
    private final AtomicBoolean abortRun;
    private final AtomicBoolean terminateRun;
    private final AtomicBoolean terminateWorkers = new AtomicBoolean();
    private final AtomicBoolean waitingSchedulerShutdown = new AtomicBoolean();
    private final int errorThreshold;
    private final Boolean warmup;
    private final Histogram histogram;
    //The number Of Significant Digits used to report latency
    private static final int numberOfSignificantValueDigits = 3;
    private static final double numberOfSignificantDigitsScale = Math.pow(10, numberOfSignificantValueDigits);
    private final Histogram queueDepthTracker;

    private final Vector<Exception> errors = new Vector<>();
    private final int callsPerSecond;
    private final OpenTelemetry openTelemetry;
    private final TinkerBench2Args cliArgs;
    private final Boolean hdrHistFmt;

    private final LogSource logger = LogSource.getInstance();

    private final List<Future<?>> schedulerFutures = new Vector<>();

    private WorkloadStatus status;
    private Progressbar progressbar = null;
    private QueryRunnable queryRunnable = null;

    private LocalDateTime startDateTime = null;
    private long startTimeNanos = 0;
    private LocalDateTime stopDateTime = null;
    private long stopTimeNanos = 0;

    public WorkloadProviderScheduler(OpenTelemetry openTelemetry,
                                     Duration targetRunDuration,
                                     boolean warmup,
                                     TinkerBench2Args cliArgs) {
        this.hdrHistFmt = cliArgs.hdrHistFmt;
        this.abortRun = cliArgs.abortRun;
        this.terminateRun = cliArgs.terminateRun;
        this.errorThreshold = cliArgs.errorsAbort;
        this.targetRunDuration =  targetRunDuration == null
                ? cliArgs.duration
                : targetRunDuration;
        this.callsPerSecond = cliArgs.queriesPerSecond;
        this.shutdownTimeout = cliArgs.shutdownTimeout;
        this.schedulers = cliArgs.schedulers;
        this.openTelemetry = openTelemetry == null ? new OpenTelemetryDummy() : openTelemetry;
        this.cliArgs = cliArgs;
        this.warmup = warmup;
        {
            // A Histogram covering the range from 1 nano to Trackable duration (max latency per query) with 3 digits resolution:
            // All latency reported must be in ns resolution
            final Duration higestTrackableDuration = Duration.ofSeconds((this.targetRunDuration.toSeconds()/this.callsPerSecond) + 5);
            this.histogram = new AtomicHistogram(higestTrackableDuration.toNanos(), numberOfSignificantValueDigits);
            if(log.isDebugEnabled()) {
                logger.PrintDebug("WorkloadProviderScheduler",
                                        "AtomicHistogram latency highestTrackableValue: %,d%n\tnumberOfSignificantValueDigits: %d%n\tFoot print: %,d (bytes)",
                                    this.histogram.getHighestTrackableValue(),
                                    this.histogram.getNumberOfSignificantValueDigits(),
                                    this.histogram.getEstimatedFootprintInBytes());
            }
            //Tack pending queries for reporting
            this.queueDepthTracker = new AtomicHistogram(this.callsPerSecond/2, 0);
            if(log.isDebugEnabled()) {
                logger.PrintDebug("WorkloadProviderScheduler",
                        "AtomicHistogram queue depth highestTrackableValue: %,d%n\tnumberOfSignificantValueDigits: %d%n\tFoot print: %,d (bytes)",
                        this.queueDepthTracker.getHighestTrackableValue(),
                        this.queueDepthTracker.getNumberOfSignificantValueDigits(),
                        this.queueDepthTracker.getEstimatedFootprintInBytes());
            }
        }

        schedulerPool = Executors.newFixedThreadPool(this.schedulers);
        workerPool = Executors.newFixedThreadPool(cliArgs.workers);

        this.openTelemetry.Reset(cliArgs,
                                null,
                                null,
                                targetRunDuration,
                                0,
                                warmup,
                                null);
        setStatus(WorkloadStatus.Initialized);
    }

    public WorkloadProviderScheduler(OpenTelemetry openTelemetry,
                                     Duration targetRunDuration,
                                     boolean warmup,
                                     TinkerBench2Args cliArgs,
                                     QueryRunnable query) {
        this(openTelemetry,
                targetRunDuration,
                warmup,
                cliArgs);
        this.setQuery(query);
    }

    /*
    Returns true if current run is a warmup.
     */
    public boolean isWarmup() { return warmup; }

    public boolean isAborted() { return abortRun.get(); }

    public boolean isDebug() { return logger.isDebug(); }

    /*
    The number of defined Schedulers.
     */
    public int getSchedulers() { return schedulers; }

    /*
    The targeted calls-per-second.
     */
    public int getTargetCallsPerSecond() { return callsPerSecond; }
    /*
    The targeted duration of executing the workload.
     */
    public Duration getTargetRunDuration() { return targetRunDuration; }
    /*
    The accumulative running duration of all operations (not the "Wall Clock" duration)
     */
    public Duration getAccumDuration() { return Duration.ofNanos(successfulDuration.get() + errorDuration.get()); }
    /*
    The "Wall Clock" Running duration.
     */
    public Duration getRunningDuration() {
        if(startTimeNanos > 0) {
            if (stopTimeNanos <= 0)
                return Duration.ofNanos(System.nanoTime() - startTimeNanos);
            return Duration.ofNanos(stopTimeNanos - startTimeNanos);
        }
        return Duration.ZERO;
    }
    /*
    The number of pending calls.
     */
    public long getPendingCount() { return pendingCount.get(); }
    /*
    Number of calls aborted
     */
    public long getAbortedCount() { return abortedCount.get(); }
    /*
    The amount of time accumulative taken for successful executions (not wall clock)
     */
    public Duration getAccumSuccessDuration() {
       return Duration.ofNanos(successfulDuration.get());
    }
    /*
    The number of success calls.
     */
    public long getSuccessCount() { return histogram.getTotalCount();}
    /*
   The amount of time accumulative taken for error execution state (not wall clock)
    */
    public Duration getAccumErrorDuration() { return Duration.ofNanos(errorDuration.get());}

    /*
    The number of errors encountered.
     */
    public long getErrorCount() { return errorCount.get(); }

    /*
    The collection of errors encountered.
     */
    public List<Exception> getErrors() { return errors.stream().toList(); }

    @Override
    public long AddError(final Exception e) {
        if(openTelemetry != null)
            openTelemetry.addException(e);
        errors.add(e);
        return errorCount.incrementAndGet();
    }

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
    Returns the remaining time of the run based on the Targeted Run Duration.
     */
    public Duration getRemainingTime() {
        if(startTimeNanos > 0) {
            if(stopTimeNanos <= 0) {
                return Duration.ofNanos(targetRunDuration.toNanos()
                                            - (System.nanoTime() - startTimeNanos));
            }
            return Duration.ZERO;
        }

        return targetRunDuration;
    }
    /*
    Returns the current Success calls-per-second rate.
     */
    public double getCallsPerSecond() {
        if(startTimeNanos > 0) {
            if(stopTimeNanos <= 0) {
                return histogram.getTotalCount() / ((System.nanoTime() - startTimeNanos) / 1_000_000_000.0);
            }
            return histogram.getTotalCount() / ((stopTimeNanos - startTimeNanos) / 1_000_000_000.0);
        }

        return 0;
    }

    /*
    Returns the latency at the provided Percentile in MS
     */
    public double getLatencyMSAtPercentile(double desiredPercentile) {
        long percentValue = histogram.getValueAtPercentile(desiredPercentile);
        return Math.round((percentValue/ Helpers.NS_TO_MS) * numberOfSignificantDigitsScale) / numberOfSignificantDigitsScale;
    }
    /*
    Returns the Percentile latency in MS and total count up to the provided Percentile
     */
    public Pair<Double,Long> getLatencyMSCountAtPercentile(double desiredPercentile) {
        long percentValue = histogram.getValueAtPercentile(desiredPercentile);
        return new Pair<Double,Long>(Math.round((percentValue/ Helpers.NS_TO_MS) * numberOfSignificantDigitsScale) / numberOfSignificantDigitsScale,
                                        histogram.getCountBetweenValues(0, percentValue));
    }
    /*
    Returns the current errors-per-second rate.
     */
    public double getErrorsPerSecond() {
        if(startTimeNanos > 0) {
            if(stopTimeNanos <= 0) {
                return errorCount.get() / ((System.nanoTime() - startTimeNanos) / 1_000_000_000.0);
            }
            return errorCount.get() / ((stopTimeNanos - startTimeNanos) / 1_000_000_000.0);
        }

        return 0;
    }

    public OpenTelemetry getOpenTelemetry() { return openTelemetry; }

    public TinkerBench2Args getCliArgs() { return cliArgs; }

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
                                    pendingCount.get(),
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
            final int useScheduler = schedulers;
            final int base = callsPerSecond / useScheduler;
            final int remainder = callsPerSecond % useScheduler;
            waitingSchedulerShutdown.set(false);
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
                        logger.warn("Pre-Process returned false and workload {} NOT executed",
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
            if(progressbar != null) {
                progressbar.close();
            }
            schedulerFutures.clear();
            System.out.printf("Starting %s for %s %s%n",
                                warmup ? "Warmup" : "Workload",
                                queryRunnable.WorkloadType().toString(),
                                queryRunnable.Name());

            progressbar = new Progressbar(this);
            progressbar.start();

            final long targetDuration = System.nanoTime() + targetRunDuration.toNanos();

            for (int i = 0; i < schedulers; i++) {
                final int rate = base + (i < remainder ? 1 : 0);
                schedulerFutures.add(schedulerPool.submit(() -> runDispatcher(rate, targetDuration)));
            }

            try {
                Thread.sleep(1000);
            }
            catch (InterruptedException ignored) {}

            return this;
        }

        if(status == WorkloadStatus.Running)
            return this;

        throw new RuntimeException("Cannot Start a Workload in state " + status);
    }

    /*
    Shuts-down the workload scheduler. If the workload is running, this will wait until completion.
    If the scheduler is already shutdown, it just returns.
     */
    public WorkloadProvider Shutdown() { close(); return this; }

    /**
     * Shutdowns the schedulers and workers waiting for proper termination...
     */
    @Override
    public void close() {

        if(status == WorkloadStatus.Shutdown
                || status == WorkloadStatus.PendingShutdown)
            return;

        if(progressbar != null) {
            progressbar.close();
            progressbar = null;
        }

        boolean alreadyCompleted = status == WorkloadStatus.Completed;

        if(queryRunnable != null) {
            System.out.printf("Pending Shutdown for %s %s%n",
                    queryRunnable.WorkloadType().toString(),
                    queryRunnable.Name());
        }

        setStatus(WorkloadStatus.PendingShutdown);
        if(!schedulerPool.isTerminated()) {
            for (Future<?> schedulerFuture : schedulerFutures) {
                schedulerFuture.cancel(true);
            }
        }
        shutdownAndAwaitTermination(workerPool, "Worker");
        shutdownAndAwaitTermination(schedulerPool, "Scheduler");
        setStatus(WorkloadStatus.Shutdown);

        if(!alreadyCompleted && queryRunnable != null) {
            System.out.printf("Running Post-process for %s %s %s...",
                                warmup ? "Warmup" : "Workload",
                                queryRunnable.WorkloadType().toString(),
                                queryRunnable.Name());
            queryRunnable.postProcess();
            System.out.println(" Completed");
        }

        if(queryRunnable != null) {
            String grafanaRange = Helpers.PrintGrafanaRangeJson(startDateTime, stopDateTime);
            if(grafanaRange != null) {
                final String msg = String.format("%s\tStarted: %s\tEnded: %s%n%s%n",
                                                    warmup ? "Warmup" : "Workload",
                                                    Helpers.GetLocalTimeZone(startDateTime),
                                                    Helpers.GetLocalTimeZone(stopDateTime),
                                                    grafanaRange);

                Helpers.Println(System.out,
                        msg,
                        Helpers.BLACK,
                        Helpers.GREEN_BACKGROUND);
                logger.info(msg);
            }
            final String msg = String.format("Shutdown for %s %s %s Completed%n",
                                            warmup ? "Warmup" : "Workload",
                                            queryRunnable.WorkloadType().toString(),
                                            queryRunnable.Name());
            System.out.println(msg);
            logger.info(msg);
        }
    }

    @Override
    public void SignalAbortWorkLoad() {
        abortRun.set(true);
    }

    /*
    Blocks until all tasks have completed normally, a shutdown requested, timeout occurred, or a worker is interrupted.

    Returns:
        true if the workload terminates normally.
     */
    public boolean awaitTermination() {

        boolean result = false;

        if(status == WorkloadStatus.Running) {
            result = true;
            logger.PrintDebug("WorkloadProviderScheduler",
                            "Awaiting for Completion...");

            LocalDateTime exitTime = LocalDateTime.now()
                                            .plus(targetRunDuration)
                                            .plus(shutdownTimeout);
            boolean abortNextTimeout = false;

            while (!schedulerPool.isTerminated()
                        && !schedulerPool.isShutdown()) {
                try {
                    shutdownLatch.await(shutdownTimeout.toSeconds(), TimeUnit.SECONDS);

                    if(LocalDateTime.now().isAfter(exitTime)) {
                        if(abortNextTimeout) {
                            abortRun.set(true);
                            schedulerPool.shutdownNow();
                            workerPool.shutdownNow();
                            System.err.printf("\tStopping %s due to Abort Signalled...%n",
                                                warmup ? "warmup" : "workload");
                            logger.warn("\tStopping %s due to Abort Signalled...",
                                            warmup ? "warmup" : "workload");
                            result = false;
                            break;
                        }
                        terminateWorkers.set(true);
                        schedulerPool.shutdown();
                        workerPool.shutdown();
                        System.out.printf("\tStopping %s due to Timeout... Waiting Completion...%n",
                                            warmup ? "warmup" : "workload");
                        logger.info("\tStopping %s due to Timeout...",
                                        warmup ? "warmup" : "workload");
                        if(workerPool.awaitTermination(shutdownTimeout.toSeconds(), TimeUnit.SECONDS)
                                &&  schedulerPool.awaitTermination(shutdownTimeout.toSeconds(), TimeUnit.SECONDS))
                        {
                            break;
                        };
                        abortNextTimeout = true;
                        exitTime =  LocalDateTime.now()
                                        .plus(shutdownTimeout);
                    }
                }
                catch (InterruptedException ignored) {
                    break;
                }
            }

            if(progressbar != null) {
                progressbar.close();
            }
            setStatus(WorkloadStatus.Completed);
            if (queryRunnable != null) {
                System.out.printf("Running Post-process for %s %s %s...",
                        warmup ? "Warmup" : "Workload",
                        queryRunnable.WorkloadType().toString(),
                        queryRunnable.Name());
                queryRunnable.postProcess();
                System.out.println(" Completed");
            }
        }
        System.out.printf("\t%s Status %s%n",
                            warmup ? "Warmup" : "Workload",
                            status);
        return result;
    }

    private static String getErrorMessage(Throwable error, int depth) {

        if(error == null) { return "<Null>"; }
        String errMsg = error.getMessage();
        if(errMsg == null) {
            if(depth > 50)
                return error.toString();
            if(error instanceof ResponseException re) {
                String msg = re.getMessage();
                Optional<String> value = re.getRemoteStackTrace();
                return String.format("%s: %sRemoteStackTrace: %s",
                                        re.getClass().getName(),
                                        msg == null ? "" : msg + " ",
                                        value.orElse(""));
            }
            return getErrorMessage(error.getCause(), depth + 1);
        }
        return errMsg;
    }

    private static String getErrorMessage(Throwable error) {
       return getErrorMessage(error, 0);
    }

    public WorkloadProvider PrintSummary(PrintStream printStream, boolean useHdrHistFmt) {

        if (queryRunnable == null) {
            printStream.printf("No %s Summary available.%n",
                    warmup ? "Warmup" : "Workload");
            return this;
        }
        //Summary Report
        {
            final long totalCount = getSuccessCount()
                    + getErrorCount()
                    + getAbortedCount();

            printStream.printf("%s Summary for %s:%n",
                    warmup ? "Warmup " : "Workload",
                    queryRunnable.Name());
            printStream.printf("\tStatus: %s%n", abortRun.get()
                    ? getStatus() + " (Signaled)"
                    : getStatus());
            printStream.printf("\tRuntime Duration: %s%n", getRunningDuration());

            printStream.println("\tSuccessful Operations");
            printStream.printf("\t\tMean OPS: %,.2f%n", getCallsPerSecond());
            printStream.printf("\t\tOperations: %,d%n", getSuccessCount());
            printStream.printf("\t\tAccumulative Duration: %s%n", getAccumSuccessDuration());

            printStream.println("\tOperation Errors");
            printStream.printf("\t\tMean OPS: %,.2f%n", getErrorsPerSecond());
            printStream.printf("\t\tErrors: %,d%n", getErrorCount());
            printStream.printf("\t\tAccumulative Duration: %s%n", getAccumErrorDuration());

            printStream.printf("\tAccumulative of all Operation Durations: %s%n", getAccumDuration());

            printStream.printf("\tAborted Operations: %,d%n", getAbortedCount());
            printStream.printf("\tTotal Number of All Operations: %,d%n", totalCount);
        }
        //Queue Depth Report
        {
            final long maxValue = this.queueDepthTracker.getMaxValue();
            double maxPercent = 0;
            if(maxValue > 0) {
                maxPercent = ((double) this.queueDepthTracker.getCountAtValue(maxValue)
                                    / (double) this.queueDepthTracker.getTotalCount())
                                * 100.0;
            }
            printStream.printf("\tQueue Depth:%n");
            printStream.printf("\t\tMean: %,d\tMax:%,d (occurs %,.2f%%)%n",
                                Math.round(this.queueDepthTracker.getMean()),
                                maxValue,
                                maxPercent);
            printStream.printf("\t\t25%% depth under %,d%n",
                                Math.round(this.queueDepthTracker.getValueAtPercentile(25.0)));
            printStream.printf("\t\t50%% depth under %,d%n",
                                Math.round(this.queueDepthTracker.getValueAtPercentile(50.0)));
            printStream.printf("\t\t75%% depth under %,d%n",
                    Math.round(this.queueDepthTracker.getValueAtPercentile(75.0)));
            printStream.printf("\t\t90%% depth under %,d%n",
                    Math.round(this.queueDepthTracker.getValueAtPercentile(90.0)));
        }

        //Error Report
        if (getErrorCount() > 0) {
            final String preFix = String.format("%n\t\t\t");

            printStream.println("Error Summary:");

            Map<String, List<Exception>> errors = getErrors()
                    .stream()
                    .collect(Collectors.groupingBy(w -> w.getClass().getName()));

            for (Map.Entry<String, List<Exception>> entry : errors.entrySet()) {
                try {
                    Map<String, List<Exception>> sameMsgs = entry.getValue()
                            .stream()
                            .collect(Collectors.groupingBy(WorkloadProviderScheduler::getErrorMessage));
                    if (entry.getValue().size() == sameMsgs.size()) {
                        printStream.printf("\tCnt: %d\tException: %s\t'%s'%n",
                                entry.getValue().size(),
                                Helpers.GetShortClassName(entry.getKey()),
                                Helpers.GetShortErrorMsg(entry.getValue().get(0).getMessage(),
                                        0,
                                        preFix,
                                        " "));
                    } else {
                        printStream.printf("\tCnt: %d\tException: %s:%n",
                                entry.getValue().size(),
                                Helpers.GetShortClassName(entry.getKey()));
                        for (Map.Entry<String, List<Exception>> sameMsg : sameMsgs.entrySet()) {
                            printStream.printf("\t\tCnt: %d\tMsg: '%s'%n",
                                    sameMsg.getValue().size(),
                                    Helpers.GetShortErrorMsg(sameMsg.getKey(),
                                            0,
                                            preFix,
                                            " "));
                        }
                    }
                } catch (Exception ignored) {
                    printStream.printf("\tCnt: %d\t%s:%n",
                            entry.getValue().size(),
                            Helpers.GetShortErrorMsg(entry.getKey(),
                                    0,
                                    preFix,
                                    " "));
                }
            }
            if (logger.loggingEnabled()) {
                printStream.println("Note: Check logs for detailed error messages and stack traces...");
            } else {
                printStream.println("Note: Enabling logging will provided detailed error messages...");
                printStream.println("\tYou can enabled logging by changing ./resouces/logback.xml file or SLF4J property settings...");
            }
        }

        //HDR Histogram Report
        {
            printStream.println();
            if (!warmup && useHdrHistFmt) {
                printStream.printf("Recorded latencies [in ms] for %s%n",
                        queryRunnable.Name());
                histogram.outputPercentileDistribution(printStream, Helpers.NS_TO_MS);
            } else {
                printStream.printf("Summary recorded latencies for %s%n",
                        queryRunnable.Name());
                final double[] desiredPercentiles = {50.0, 90.0, 95.0, 99.0, 99.9};
                final double scale = numberOfSignificantDigitsScale;

                printStream.printf("\t\tPercentile\tValue [ms]\tCount%n");
                for (double desiredPercentile : desiredPercentiles) {
                    final Pair<Double, Long> latencyCnt = getLatencyMSCountAtPercentile(desiredPercentile);

                    printStream.printf("\t\t%,.2f\t\t%,.3f\t\t%,d%n",
                            desiredPercentile,
                            latencyCnt.getValue0(),
                            latencyCnt.getValue1());
                }
                final double meanValue = histogram.getMean();
                final double meanRoundedValue = Math.round((meanValue / Helpers.NS_TO_MS) * scale) / scale;
                final long maxValue = histogram.getMaxValue();
                final double maxRoundedValue = Math.round((maxValue / Helpers.NS_TO_MS) * scale) / scale;
                final double stdValue = histogram.getStdDeviation();
                final double stdRoundedValue = Math.round((stdValue / Helpers.NS_TO_MS) * scale) / scale;

                printStream.printf("Mean is %,.3fms%nMaximum is %,.3fms%nStdDeviation is %,.3f%n",
                        meanRoundedValue,
                        maxRoundedValue,
                        stdRoundedValue);
            }
        }
        printStream.println();
        return this;
    }

    public WorkloadProvider PrintSummary() {

        try {
            Thread.sleep(1000);
        } catch (InterruptedException ignored) { }

        if(logger.getLogger4j().isInfoEnabled()) {
            //Print to log file
            try (LogSource.Stream logStream = new LogSource.Stream(logger)) {
                PrintSummary(logStream.getPrintStream(), true);
                logStream.info();
            }
        }

        //Make sure the progression bar is closed!
        if(progressbar != null) {
            progressbar.close();
        }

        System.out.flush();
        System.err.flush();
        System.out.println();

        try {
            System.out.print(Helpers.YELLOW_BACKGROUND);
            System.out.print(Helpers.BLACK);
            PrintSummary(System.out, this.hdrHistFmt);
            System.out.flush();
            System.err.flush();
        }
        finally {
            System.out.print(Helpers.RESET);
        }
        System.out.println();
        return this;
    }

    @Override
    public String toString() {

        if(startTimeNanos == 0)
        {
            return String.format("WorkloadProvider [type=%s, status=%s, call/sec=, pending=%,d, successes=%,d, errors=%,d,aborted=%d, start=, stop=, run_duration=0, run_reminding=%s]",
                    warmup ? "warmup" : "workload",
                    status,
                    getPendingCount(),
                    getSuccessCount(),
                    getErrorCount(),
                    getAbortedCount(),
                    targetRunDuration);
        }

        return String.format("WorkloadProviderScheduler [type=%s, status=%s, call/sec=%,.2f, pending=%,d, successes=%,d, errors=%,d, aborted=%d, start=%s, stop=%s, run_duration=%s, run_reminding=%s]",
                                warmup ? "warmup" : "workload",
                                status,
                                getCallsPerSecond(),
                                getPendingCount(),
                                getSuccessCount(),
                                getErrorCount(),
                                getAbortedCount(),
                                startDateTime == null ? "N/A" : Helpers.GetDateTimeString(startDateTime),
                                stopDateTime == null ? "N/A" : Helpers.GetDateTimeString(stopDateTime),
                                getAccumDuration(),
                                getRemainingTime());
    }

    private void setStatus(WorkloadStatus workloadStatus) {

        if(this.status != workloadStatus) {
            this.status = workloadStatus;

            if (workloadStatus == WorkloadStatus.Running && startDateTime == null) {
                startTimeNanos = System.nanoTime();
                startDateTime = LocalDateTime.now();
            }
            else if (startDateTime != null
                        && stopDateTime == null
                        && (workloadStatus == WorkloadStatus.Completed
                            || workloadStatus == WorkloadStatus.PendingShutdown
                            || workloadStatus == WorkloadStatus.Shutdown)) {
                stopTimeNanos = System.nanoTime();
                stopDateTime = LocalDateTime.now();
            }

            if (openTelemetry != null)
                openTelemetry.setConnectionState(workloadStatus.toString());

            logger.PrintDebug("WorkloadProviderScheduler", "New Status %s for %s",
                    workloadStatus,
                    queryRunnable);
        }
    }

    private final class Handler implements Runnable {

        final long HighestTrackableValue = histogram.getHighestTrackableValue();
        final long HighestTrackableValueDepth = queueDepthTracker.getHighestTrackableValue();

        Handler() {  }

        private void RecordLatency(long latency) {
            if (latency < HighestTrackableValue) {
                histogram.recordValue(latency);
            } else {
                histogram.recordValue(HighestTrackableValue - 1);

                logger.Print("Handler.RecordLatency",
                            true,
                            "Latency Value %,d ns was too large to record. Using Highest Trackable Value of %,d ns...",
                            latency,
                            HighestTrackableValue - 1);
            }
        }

        private void RecordDepth(long depth) {
            if (depth < HighestTrackableValueDepth) {
                queueDepthTracker.recordValue(depth);
            } else {
                String msg = String.format("Query Queue Depth of %,d is too large! Maximum dept is %,d. Stopping execution...",
                                            depth,
                                            HighestTrackableValueDepth);
                System.err.println(msg);
                logger.error(msg);
                abortRun.set(true);
                Exception e = new RuntimeException(msg);
                Error(0, e);
            }
        }

        private void Success(long latency) {
            successfulDuration.addAndGet(latency);
            RecordLatency(latency);
            if(openTelemetry != null) {
                openTelemetry.recordElapsedTime(latency);
            }
        }

        private void Error(long latency, Exception e) {
            AddError(e);
            if(latency > 0) {
                errorDuration.addAndGet(latency);
            }
            logger.PrintDebug("WorkloadProviderScheduler.Handler",
                                e);
            logger.error(String.format("%s %s",
                                        isWarmup() ? "Warmup" : "Workload",
                                        queryRunnable.Name()), e);
        }

        public void run() {
            long startCall = 0;
            Object recordResult = null;
            boolean success = false;
            Exception lastError = null;

            if(abortRun.get()) return;

            pendingCount.incrementAndGet();
            openTelemetry.incrPendingTransCounter();

            try {
                queryRunnable.preCall();
                if(abortRun.get()) return;

                startCall = System.nanoTime();
                final Pair<Boolean, Object> callResult = queryRunnable.call();
                final long duration = System.nanoTime() - startCall;
                recordResult = callResult.getValue1();
                if (!terminateWorkers.get()) {
                    if (callResult.getValue0()) {
                        Success(duration);
                        success = true;
                    } else {
                        abortedCount.incrementAndGet();
                        logger.warn("Workload {} aborted",
                                    queryRunnable);
                    }
                }
            } catch (InterruptedException e) {
                abortedCount.incrementAndGet();
            } catch (TraversalInterruptedException e) {
                final long duration = System.nanoTime() - startCall;
                if(terminateWorkers.get()) {
                    abortedCount.incrementAndGet();
                } else {
                    lastError = e;
                    Error(startCall == 0 ? 0 : duration,
                            e);
                }
            } catch (CompletionException e) {
                final long duration = System.nanoTime() - startCall;
                lastError = (Exception) e.getCause();
                Error(startCall == 0 ? 0 : duration,
                        lastError);
            } catch (Exception e) {
                final long duration = System.nanoTime() - startCall;
                lastError = e;
                Error(startCall == 0 ? 0 : duration,
                        e);
            } finally {
                try {
                    queryRunnable.postCall(recordResult,
                                            success,
                                            lastError);
                } catch (Exception e) {
                    if(!terminateWorkers.get()) {
                        Error(0, e);
                    }
                }
                RecordDepth(pendingCount.decrementAndGet());
                openTelemetry.decrPendingTransCounter();
                progressbar.step();
            }
        }
    }

    private void runDispatcher(final int rate,
                               final long targetDurationNS) {

        final long callIntervalNS = 1_000_000_000L / rate;
        long nextCallTime = System.nanoTime();
        setStatus(WorkloadStatus.Running);
        long now;

        while ((now = System.nanoTime()) <= targetDurationNS
                && errorCount.get() <= errorThreshold
                && !terminateWorkers.get()
                && !abortRun.get()
                && !terminateRun.get()) {
            if (now >= nextCallTime) {
                workerPool.execute(new Handler());
                nextCallTime += callIntervalNS;
            } else {
                Thread.onSpinWait();
            }
        }

        terminateWorkers.set(true);

        if(errorCount.get() > errorThreshold
                && !abortRun.get())
        {
            abortRun.set(true);
            progressbar.stop();
            System.err.printf("\tStopping %s due %d Errors for %s...%n",
                                warmup ? "warmup" : "workload",
                                errorCount.get(),
                                queryRunnable.Name());
            logger.warn("Stopping {} {} due to the number of errors ({})",
                            warmup ? "warmup" : "workload",
                            queryRunnable.Name(),
                            errorCount.get());
        }

        setStatus(WorkloadStatus.WaitingCompletion);
        if(!waitingSchedulerShutdown.get()) {
            waitingSchedulerShutdown.set(true);
            progressbar.stop();
            System.out.printf("\tStopping %s due to %s...%n",
                    warmup ? "warmup" : "workload",
                    abortRun.get() ? "Signal" : "Duration Reached");
        }
        schedulerPool.shutdownNow();
        workerPool.shutdownNow();
        shutdownLatch.countDown();
        logger.PrintDebug("WorkloadProviderScheduler",
                        "Aborting Workers %s",
                        queryRunnable);
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

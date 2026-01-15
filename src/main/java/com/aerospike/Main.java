package com.aerospike;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import picocli.CommandLine;

public class Main extends TinkerBenchArgs {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final AtomicInteger exitStatus = new AtomicInteger(0);
    private static QueryRunnable workloadRunnerCache;

    private static void ExecuteWorkload(OpenTelemetry openTel,
                                        LogSource logger,
                                        AGSGraphTraversal agsGraphTraversal,
                                        IdManager idManager,
                                        Duration targetRunDuration,
                                        TinkerBenchArgs args,
                                        int qps,
                                        boolean isWarmUp,
                                        boolean ranWarmUp) {

        try (final WorkloadProvider workload = new WorkloadProviderScheduler(openTel,
                                                                            targetRunDuration,
                                                                            qps,
                                                                            isWarmUp,
                                                                            ranWarmUp,
                                                                            args)) {
            final boolean isQueryString = args.queryNameOrString
                                            .indexOf(".") > 0;

            final QueryRunnable workloadRunner = workloadRunnerCache == null
                                                    ? (isQueryString
                                                        ? new EvalQueryWorkloadProvider(workload,
                                                                                        agsGraphTraversal,
                                                                                        args.queryNameOrString,
                                                                                        idManager)
                                                        : Helpers.GetQuery(args.queryNameOrString,
                                                                            workload,
                                                                            agsGraphTraversal,
                                                                            idManager,
                                                                            args.debug))
                                                    : workloadRunnerCache.SetWorkloadProvider(workload);

            if (mainInstance.abortRun.get())
                return;

            if(!args.appTestMode
                    && args.idManager.enabled()) {
                if (!args.idManager.isInitialized()) {
                    final int sampleSIze = workloadRunner.getSampleSize() < 0
                            ? args.idSampleSize
                            : workloadRunner.getSampleSize();
                    final String[] labels = workloadRunner.getSampleLabelId() == null
                            ? args.labelsSample
                            : (args.labelsSample == null
                            ? workloadRunner.getSampleLabelId()
                            : args.labelsSample);
                    if (args.importIdsPath != null) {
                        System.out.printf("Importing Ids from '%s'...%n",
                                args.importIdsPath);
                        args.idManager.importFile(args.importIdsPath,
                                openTel,
                                logger,
                                sampleSIze,
                                labels);
                    } else {
                        if (args.idManager instanceof IdManagerQuery idQuery) {
                            idQuery.init(agsGraphTraversal,
                                    openTel,
                                    logger,
                                    args.idGremlinQuery);
                        } else {
                            args.idManager.init(agsGraphTraversal.G(),
                                    openTel,
                                    logger,
                                    sampleSIze,
                                    labels);
                        }
                    }
                    args.idManager.CheckIdsExists(logger);

                    if (args.exportIdsPath != null) {
                        args.idManager.exportFile(args.exportIdsPath,
                                logger);
                    }

                    if (!args.idManager.isEmpty()) {
                        args.idManager.printStats(logger);
                    }
                }
            } else {
                logger.PrintDebug("Id Manager", "Id Manager disabled");
                openTel.setIdMgrGauge(null, null, null, -1, -1, 0, 0, 0, 0);
            }

            workloadRunner.PrepareCompile();

            if (mainInstance.abortRun.get())
                return;

            workloadRunnerCache = workloadRunner;

            if (isWarmUp) {
                System.out.println("Preparing WarmUp...");
                logger.info("Preparing WarmUp...");

                workloadRunner
                        .Start()
                        .awaitTermination()
                        .PrintSummary();

                System.out.println("WarmUp Completed...");
                logger.info("WarmUp Completed...");
            } else {
                System.out.printf("Preparing workload %s...\n", args.queryNameOrString);
                logger.info("Preparing workload {}...", args.queryNameOrString);

                workloadRunner
                        .Start()
                        .awaitTermination()
                        .PrintSummary();

                System.out.println("Workload Completed...");
                logger.info("Workload Completed...");
            }

        } catch (Exception e) {
            args.errorRun.set(true);
            logger.error(isWarmUp ? "Warmup" : "Workload", e);
            throw new RuntimeException(e);
        }
    }

    public Integer call() throws Exception {
        if(ListPredefinedQueries()) {
            exitStatus.set(1);
            return 1;
        }
        if(ListIdManagers()) {
            exitStatus.set(1);
            return 1;
        }

        final LogSource logger = new LogSource(debug);

        try {
            validate();
        } catch (Exception e) {
            logger.error("CLI Exception", e);
            PrintArguments(true);
            System.err.println("Command Line Error:%n\t" + e.getMessage());
            exitStatus.set(1);
            return 1;
        }

        PrintArguments(false);

        logger.title(this);

        System.out.printf("Application PID: %s%n", Helpers.GetPid());

        Helpers.checkJavaVersion(logger);

        if(logger.loggingEnabled()) {
            Helpers.Println(System.out,
                    "Logging Enabled",
                    Helpers.BLACK,
                    Helpers.GREEN_BACKGROUND);
        }

        final LocalDateTime appStartTime = LocalDateTime.now();

        try (final OpenTelemetry openTel = OpenTelemetryHelper.Create(this, null);
            final AGSGraphTraversalSource agsGraphTraversalSource
                            = new AGSGraphTraversalSource(this, openTel)) {

            boolean ranWarmup = false;
            if (!warmupDuration.isZero()) {
                ExecuteWorkload(openTel,
                                    logger,
                                    agsGraphTraversalSource,
                                    this.idManager,
                                    warmupDuration,
                                this,
                                    this.queriesPerSecond,
                                true,
                            false);
                ranWarmup = true;
            }

            if (!(abortRun.get()
                    || errorRun.get())) {

                if(incrQPS > 0) {
                    Helpers.Println(System.out,
                            String.format("Incrementing QPS by %s",
                                    Helpers.FmtInt(incrQPS)),
                            Helpers.BLACK,
                            Helpers.GREEN_BACKGROUND);
                    logger.info(String.format("Incrementing QPS: %d", endQPS));

                    if (endQPS <= 0) {
                        Helpers.Println(System.out,
                                "Warning: Incrementing QPS until Target QPS is not met.",
                                Helpers.RED,
                                Helpers.YELLOW_BACKGROUND);
                        logger.warn("Incrementing QPS until Target QPS is not met.");
                    }
                }

                int currentQPS = this.queriesPerSecond;
                final int endQPS = this.endQPS <= 0 ? Integer.MAX_VALUE : this.endQPS;

                Helpers.Println(System.out,
                        String.format("Target QPS of %s",
                                Helpers.FmtInt(currentQPS)),
                        Helpers.BLACK,
                        Helpers.GREEN_BACKGROUND);
                logger.info(String.format("Target QPS: %d", currentQPS));

                do {
                    ExecuteWorkload(openTel,
                                    logger,
                                    agsGraphTraversalSource,
                                    this.idManager,
                                    duration,
                                    this,
                                    currentQPS,
                                    false,
                                    ranWarmup);

                    if(incrQPS <= 0
                        || errorRun.get()
                        || abortRun.get()
                        || abortSIGRun.get()
                        || qpsErrorRun.get()
                        || terminateRun.get()) {
                        break;
                    }
                    currentQPS += incrQPS;
                    if(currentQPS <= endQPS) {
                        ranWarmup = true;
                        Helpers.Println(System.out,
                                String.format("Changing Target QPS to %s",
                                        Helpers.FmtInt(currentQPS)),
                                Helpers.BLACK,
                                Helpers.GREEN_BACKGROUND);
                        logger.info(String.format("Changing Target QPS: %d", currentQPS));
                    }
                } while (currentQPS <= endQPS);

                terminateRun.set(true);
            }
        } catch (Exception e) {
            errorRun.set(true);
            e.printStackTrace(System.err);
        } finally {

            if(errorRun.get()) {
                Helpers.Println(System.out,
                        "TinkerBench ended because of an error!",
                        Helpers.BLACK,
                        Helpers.RED_BACKGROUND);
                exitStatus.set(4);
            } else if(qpsErrorRun.get()) {
                Helpers.Println(System.out,
                        "TinkerBench ended because QPS wasn't obtained!",
                        Helpers.BLACK,
                        Helpers.RED_BACKGROUND);
                exitStatus.set(3);
            } else if(abortRun.get() || abortSIGRun.get()) {
                Helpers.Println(System.out,
                        "TinkerBench ended in Aborted Status!",
                        Helpers.BLACK,
                        Helpers.RED_BACKGROUND);
                exitStatus.set(5);
            } else {
                Helpers.Println(System.out,
                        "TinkerBench Workload Completed!",
                        Helpers.BLACK,
                        Helpers.GREEN_BACKGROUND);
            }

            final LocalDateTime appEndTime = LocalDateTime.now();
            String msg = String.format("Application\tStarted: %s\tEnded: %s%n",
                                                Helpers.GetLocalTimeZone(appStartTime),
                                                Helpers.GetLocalTimeZone(appEndTime));
            final String grafanaRange = Helpers.PrintGrafanaRangeJson(appStartTime,
                                                                    appEndTime);
            if (grafanaRange != null) {
                msg += String.format("%s%n", grafanaRange);
            }

            Helpers.Println(System.out,
                    msg,
                    Helpers.BLACK,
                    Helpers.GREEN_BACKGROUND);
            logger.info(msg);
        }

        return exitStatus.get();
    }

    private static final Main mainInstance = new Main();

    public static void main(final String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (mainInstance.terminateRun.get()) {
                System.out.println("Shutdown initiated...");
            } else {
                mainInstance.abortRun.set(true);
                mainInstance.abortSIGRun.set(true);
                System.out.println("Abort initiated. Performing cleanup...");
                exitStatus.set(5);
            }
        }));

        new CommandLine(mainInstance).execute(args);
        if(exitStatus.get() == 0) {
            System.out.println("Exiting successfully.");
        } else {
            Helpers.Println(System.out,
                    String.format("Exiting with errors. Code: %d", exitStatus.get()),
                    Helpers.BLACK,
                    Helpers.YELLOW_BACKGROUND);
        }
        System.exit(exitStatus.get());
    }
}
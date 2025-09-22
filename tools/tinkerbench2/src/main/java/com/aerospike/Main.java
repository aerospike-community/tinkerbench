package com.aerospike;

import picocli.CommandLine;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.concurrent.atomic.AtomicReference;

public class Main extends TinkerBench2Args {

    private static void ExecuteWorkload(OpenTelemetry openTel,
                                        LogSource logger,
                                        AGSGraphTraversal agsGraphTraversal,
                                        IdManager idManager,
                                        Duration targetRunDuration,
                                        TinkerBench2Args args,
                                        boolean isWarmUp) {

        try (final WorkloadProvider workload = new WorkloadProviderScheduler(openTel,
                                                                            targetRunDuration,
                                                                            isWarmUp,
                                                                            args)) {
            final boolean isQueryString = args.queryNameOrString
                                            .indexOf(".") > 0;

            final QueryRunnable workloadRunner = isQueryString
                                                    ? new EvalQueryWorkloadProvider(workload,
                                                                                    agsGraphTraversal,
                                                                                    args.queryNameOrString,
                                                                                    idManager)
                                                    : Helpers.GetQuery(args.queryNameOrString,
                                                                        workload,
                                                                        agsGraphTraversal,
                                                                        idManager,
                                                                        args.debug);
            if (mainInstance.abortRun.get())
                return;

            if(!args.appTestMode && !args.idManager.isInitialized()) {
                if(args.importIdsPath != null) {
                    args.idManager.importFile(args.importIdsPath,
                                                openTel,
                                                logger,
                                                workloadRunner.getSampleSize() < 0
                                                        ? args.idSampleSize
                                                        : workloadRunner.getSampleSize(),
                                                workloadRunner.getSampleLabelId() == null
                                                        ? args.labelSample
                                                        : (args.labelSample == null
                                                        ? workloadRunner.getSampleLabelId()
                                                        : args.labelSample));
                } else {
                    args.idManager.init(agsGraphTraversal.G(),
                                        openTel,
                                        logger,
                                        workloadRunner.getSampleSize() < 0
                                                ? args.idSampleSize
                                                : workloadRunner.getSampleSize(),
                                        workloadRunner.getSampleLabelId() == null
                                                ? args.labelSample
                                                : (args.labelSample == null
                                                ? workloadRunner.getSampleLabelId()
                                                : args.labelSample));
                }
            }

            workloadRunner.PrepareCompile();

            if (mainInstance.abortRun.get())
                return;

            if (isWarmUp) {
                System.out.println("Preparing WarmUp...");
                logger.info("Preparing WarmUp...");
            } else {
                System.out.printf("Preparing workload %s...\n", args.queryNameOrString);
                logger.info("Preparing workload {}...", args.queryNameOrString);
            }

            workloadRunner
                .Start()
                .awaitTermination()
                .PrintSummary();

            if (isWarmUp) {
                System.out.println("WarmUp Completed...");
                logger.info("WarmUp Completed...");
            } else {
                System.out.println("Workload Completed...");
                logger.info("Workload Completed...");
            }

        } catch (Exception e) {
            logger.error(isWarmUp ? "Warmup" : "Workload", e);
            throw new RuntimeException(e);
        }
    }

    public Integer call() throws Exception {
        if(ListPredefinedQueries()) {
            return 0;
        }
        validate();

        PrintArguments(false);

        LogSource logger = new LogSource(debug);
        logger.title(this);

        System.out.printf("Application PID: %s%n", Helpers.GetPid());

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

            if (!warmupDuration.isZero()) {
                ExecuteWorkload(openTel,
                                    logger,
                                    agsGraphTraversalSource,
                                    this.idManager,
                                    warmupDuration,
                                this,
                            true);
            }

            if (!mainInstance.abortRun.get()) {
                ExecuteWorkload(openTel,
                                logger,
                                agsGraphTraversalSource,
                                this.idManager,
                                duration,
                                this,
                                false);
                mainInstance.terminateRun.set(true);
            }
        } finally {

            if(abortRun.get() || abortSIGRun.get()) {
                Helpers.Println(System.out,
                        "TinkerBench2 ended in Aborted Status!",
                        Helpers.BLACK,
                        Helpers.RED_BACKGROUND);
            } else {
                Helpers.Println(System.out,
                        "TinkerBench2 Workload Completed!",
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

        return  0;
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
            }
        }));

        int statusCode = new CommandLine(mainInstance).execute(args);
        System.exit(statusCode);
    }
}
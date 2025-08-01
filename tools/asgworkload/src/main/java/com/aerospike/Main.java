package com.aerospike;

import picocli.CommandLine;

import java.time.Duration;

public class Main extends  AGSWorkloadArgs {

    private static void ExecuteWorkload(OpenTelemetry openTel,
                                        LogSource logger,
                                        AGSGraphTraversal agsGraphTraversal,
                                        IdManager idManager,
                                        Duration targetRunDuration,
                                        AGSWorkloadArgs args,
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
        PrintArguments(false);

        LogSource logger = new LogSource(debug);
        logger.title(this);

        try (final OpenTelemetry openTel = OpenTelemetryHelper.Create(this, null);
            final AGSGraphTraversalSource agsGraphTraversalSource
                            = new AGSGraphTraversalSource(this, openTel)) {

            if(!this.testMode && !this.idManager.isInitialized())
                this.idManager.init(agsGraphTraversalSource.G(), idSampleSize, labelSample);

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
        }

        return  0;
    }

    private static final Main mainInstance = new Main();

    public static void main(final String[] args) {

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if (mainInstance.terminateRun.get()) {
                System.out.println("Shutdown initiated...");
            } else {
                System.out.println("Abort initiated. Performing cleanup...");
                mainInstance.abortRun.set(true);
                mainInstance.abortSIGRun.set(true);
            }
        }));

        int statusCode = new CommandLine(mainInstance).execute(args);
        System.exit(statusCode);
    }
}
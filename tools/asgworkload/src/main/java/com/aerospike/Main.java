package com.aerospike;

import picocli.CommandLine;

import java.time.Duration;

public class Main extends  AGSWorkloadArgs {

    private static void ExecuteWorkload(OpenTelemetry openTel,
                                        LogSource logger,
                                        AGSGraphTraversal agsGraphTraversal,
                                        Duration targetRunDuration,
                                        AGSWorkloadArgs args,
                                        boolean isWarmUp) {
        try(final WorkloadProvider workload = new WorkloadProviderScheduler(openTel,
                                                                            targetRunDuration,
                                                                            isWarmUp,
                                                                            args)) {
            final QueryRunnable workloadRunner = Helpers.GetQuery(args.queryName,
                                                                    workload,
                                                                    agsGraphTraversal,
                                                                    args.debug);
            if(isWarmUp) {
                System.out.println("Running WarmUp...");
                logger.info("Running WarmUp...");
            } else {
                System.out.printf("Running workload %s...\n", args.queryName);
                logger.info("Running WarmUp {}...", args.queryName);
            }

            workloadRunner
                .Start()
                .awaitTermination()
                .PrintSummary();

            if(isWarmUp) {
                System.out.println("WarmUp Completed...");
                logger.info("WarmUp Completed...");
            } else {
                System.out.println("Workload Completed...");
                logger.info("Workload Completed...");
            }

        } catch (Exception e) {
            logger.error(isWarmUp ? "Warmup" : "Workload",e);
            throw new RuntimeException(e);
        }
    }

    public Integer call() throws Exception {
        PrintArguments(false);

        LogSource logger = new LogSource(debug);
        logger.title(this);

        try(final OpenTelemetry openTel = OpenTelemetryHelper.Create(this, null);
            final AGSGraphTraversalSource agsGraphTraversalSource
                            = new AGSGraphTraversalSource(this, openTel)) {

            if (!warmupDuration.isZero()) {
                ExecuteWorkload(openTel,
                                    logger,
                                    agsGraphTraversalSource,
                                    warmupDuration,
                                this,
                            true);
            }

            if(!mainInstance.abortRun.get()) {
                ExecuteWorkload(openTel,
                        logger,
                        agsGraphTraversalSource,
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
            if(mainInstance.terminateRun.get()) {
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
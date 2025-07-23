package com.aerospike;

import picocli.CommandLine;

import java.io.IOException;

public class Main extends  AGSWorkloadArgs {

    public Integer call() throws Exception {
        PrintArguments(false);

        LogSource logger = new LogSource(debug);
        logger.title(this);

        final OpenTelemetry openTel = OpenTelemetryHelper.Create(this,
                                                                null);
        final WorkloadProvider workload = new WorkloadProviderScheduler(openTel,
                                                                        this);
        final AGSGraphTraversal agsGraphTraversalSource = new AGSGraphTraversalSource(this,
                                                                                        openTel);

        try {
            final QueryRunnable workloadRunner = Helpers.GetQuery(queryName,
                                                                    workload,
                                                                    agsGraphTraversalSource,
                                                                    debug);
            workloadRunner
                    .Start()
                    .awaitTermination();
            workloadRunner.PrintSummary();

        } catch (Exception e) {
            logger.error("main",e);
            throw new RuntimeException(e);
        }
        finally {
            workload.close();
            agsGraphTraversalSource.close();
            openTel.close();
        }

        logger.getLogger4j().info("closed");

        return  0;
    }

    public static void main(final String[] args) {

        Main mainInstance = new Main();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown initiated. Performing cleanup...");
            if(!mainInstance.terminateRun.get())
                mainInstance.abortRun.set(true);
        }));

        int statusCode = new CommandLine(mainInstance).execute(args);
        System.exit(statusCode);
    }
}
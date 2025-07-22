package com.aerospike;

import picocli.CommandLine;

public class Main extends  AGSWorkloadArgs {

    public Integer call() {
        PrintArguments(false);

        LogSource logger = new LogSource(debug);
        logger.title(this);

        try(OpenTelemetry openTel = OpenTelemetryHelper.Create(this,
                                                                null);
                WorkloadProvider workload = new WorkloadProviderScheduler(openTel,
                                                                            this);
                AGSGraphTraversal agsGraphTraversalSource = new AGSGraphTraversalSource(this,
                                                                                        openTel);
                QueryRunnable workloadRunner = Helpers.GetQuery(queryName,
                                                                workload,
                                                                agsGraphTraversalSource,
                                                                debug)) {
            workload.Start()
                    .awaitTermination();
        } catch (Exception e) {
            logger.error("main",e);
            throw new RuntimeException(e);
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
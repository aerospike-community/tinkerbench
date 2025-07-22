package com.aerospike;

import picocli.CommandLine;

public class Main extends  AGSWorkloadArgs {

    public Integer call() {
        PrintArguments(false);

        try(OpenTelemetry openTel = OpenTelemetryHelper.Create(promPort,
                                                                this,
                                                                (int) closeWaitSecs.toMillis(),
                                                                null);
                WorkloadProvider workload = new WorkloadProviderScheduler(schedulars,
                                                                            workers,
                                                                            duration,
                                                                            callsPerSecond,
                                                                            shutdownTimeout,
                                                                            openTel,
                                                                            this)) {
            workload.setQuery(new QueryTest())
                    .Start()
                    .awaitTermination();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
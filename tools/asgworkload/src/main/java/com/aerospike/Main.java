package com.aerospike;

import picocli.CommandLine;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Main extends  AGSWorkloadArgs {

    public static final AtomicBoolean abortRun = new AtomicBoolean(false);
    public static final AtomicBoolean terminateRun = new AtomicBoolean(false);

    public Integer call() {
        PrintArguments(false);

        try(OpenTelemetry openTel = OpenTelemetryHelper.Create(promPort,
                                                                (AGSWorkloadArgs) this,
                                                                promCloseWaitSecs,
                                                                null);
                WorkloadProvider workload = new WorkloadProvider(schedulars,
                                                                workers,
                                                                duration,
                                                                callsPerSecond,
                                                                shutdownTimeout,
                                                                openTel)) {
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

        int statusCode = new CommandLine(mainInstance).execute(args);
        System.exit(statusCode);
    }
}
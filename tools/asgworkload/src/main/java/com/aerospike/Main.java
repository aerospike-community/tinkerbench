package com.aerospike;

import picocli.CommandLine;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Main extends  AGSWorkloadArgs {

    public Integer call() {
        PrintArguments(false);

        try(WorkloadProvider workload = new WorkloadProvider(schedulars,
                                                                workers,
                                                                duration,
                                                                callsPerSecond,
                                                                shutdownTimeout)) {

            workload.setQuery(new QueryTest())
                    .Start()
                    .awaitTermination();
        }
        return  0;
    }

    public static void main(final String[] args) {

        Main mainInstance = new Main();

        int statusCode = new CommandLine(mainInstance).execute(args);
        System.exit(statusCode);
    }
}
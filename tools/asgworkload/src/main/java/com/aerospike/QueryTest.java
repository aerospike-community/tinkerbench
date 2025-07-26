package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class QueryTest implements QueryRunnable {

    private final WorkloadProvider provider;
    private final boolean isPrintResults;

    public QueryTest(WorkloadProvider provider,
                     AGSGraphTraversal ags) {
        this.provider = provider;
        this.provider.setQuery(this);
        this.isPrintResults = provider.getCliArgs().printResult;
    }

    @Override
    public String Name() { return "queryTest"; }

    @Override
    public WorkloadTypes WorkloadType() {
        return WorkloadTypes.Test;
    }

    @Override
    public boolean isWarmup() { return provider.isWarmup(); }

    @Override
    public boolean isPrintResult() { return isPrintResults; }

    @Override
    public <T> void PrintResult(T result) {
        System.out.println(result);
    }

    /**
     * @return the Workload description
     */
    @Override
    public String getDescription() {
        return "Test Query";
    }

    @Override
    public QueryRunnable Start() {
        provider.Start();
        return this;
    }

    @Override
    public QueryRunnable awaitTermination() {
        this.provider.awaitTermination();
        return this;
    }

    @Override
    public QueryRunnable Shutdown() {
        this.provider.Shutdown();
        return this;
    }

    @Override
    public QueryRunnable PrintSummary() {
        provider.PrintSummary();
        return this;
    }

    /**
     * @return true to indicate to execute the workload and false to abort.
     */
    @Override
    public boolean preProcess() {
        System.out.println("PreProcess QueryTest");
        System.out.println(provider);
        return true;
    }

    /**
     *
     */
    @Override
    public void postProcess() {
        System.out.println("PostProcess QueryTest");
        System.out.println(provider);
    }

    /**
     * Performs the actual workload
     * @return True to measure the workload and false to indicate the workload was aborted.
     */
    @Override
    public Boolean call() throws InterruptedException {
        System.out.println("Running QueryTest");
        System.out.println(provider);
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.out.println("QueryTest Exception " + e.getMessage());
            throw e;
        }
        return true;
    }

    /**
     * @return the AGS Graph Traversal instance
     */
    @Override
    public GraphTraversalSource G() {
        return null;
    }

    /**
     * @return the AGS cluster instance
     */
    @Override
    public Cluster getCluster() {
        return null;
    }
}

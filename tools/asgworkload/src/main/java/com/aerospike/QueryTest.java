package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.javatuples.Pair;

public class QueryTest implements QueryRunnable {

    private final WorkloadProvider provider;
    private final boolean isPrintResults;

    public QueryTest(final WorkloadProvider provider,
                     final AGSGraphTraversal ignored0,
                     final IdManager ignored1) {
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
     * @return
     */
    @Override
    public Object getVId() {
        return null;
    }

    /**
     * @return true to indicate to execute the workload and false to abort.
     */
    @Override
    public boolean preProcess() {
        System.out.println("\n**** PreProcess QueryTest");
        System.out.println(provider);
        return true;
    }

    /**
     *
     */
    @Override
    public void postProcess() {
        System.out.println("\n*****PostProcess QueryTest");
        System.out.println(provider);
    }

    /**
     *
     */
    @Override
    public void preCall() {

    }

    @Override
    public void postCall(Object ignored0, Boolean ignored1, Throwable ignored2) {

    }

    @Override
    public Pair<Boolean,Object> call() throws InterruptedException {
        try {
            Thread.sleep(1);
        } catch (InterruptedException e) {
            System.out.println("QueryTest Exception " + e.getMessage());
            throw e;
        }
        return new Pair<>(true,null);
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

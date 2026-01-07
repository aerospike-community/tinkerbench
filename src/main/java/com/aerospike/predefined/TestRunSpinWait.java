package com.aerospike.predefined;

import com.aerospike.*;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.javatuples.Pair;

public class TestRunSpinWait implements QueryRunnable {

    private final WorkloadProvider provider;
    private final boolean isPrintResults;

    public TestRunSpinWait(final WorkloadProvider provider,
                           final AGSGraphTraversal ignored0,
                           final IdManager ignored1) {

        this.provider = provider;
        if(this.provider == null) {
            this.isPrintResults = false;
        } else {
            this.provider.setQuery(this);
            this.isPrintResults = this.provider.getCliArgs().printResult;
        }
    }

    @Override
    public String Name() { return "TestRunSpinWait"; }

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
        return "Test Run that doesn't query the AGS but calls a Spin Wait.";
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
    public void PrepareCompile() {}

    @Override
    public QueryRunnable PrintSummary() {
        provider.PrintSummary();
        return this;
    }

    /*
    Returns null since sampling is not used.
     */
    @Override
    public String[] getSampleLabelId() { return  null; }

    /*
    Returns zero to disable id sampling.
     */
    @Override
    public int getSampleSize() { return 0; }

    /**
     * @return null since this does not use a sampling id...
     */
    @Override
    public Object getVId() {
        return null;
    }

    /**
     * @param depth ignored
     * @return null since it doesn't use sampling ids...
     */
    @Override
    public Object getVId(int depth) {
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
    public final void preCall() {

    }

    @Override
    public final void postCall(Object ignored0, Boolean ignored1, Throwable ignored2) {

    }

    @Override
    public final Pair<Boolean,Object> call() throws InterruptedException {
        Thread.onSpinWait();
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

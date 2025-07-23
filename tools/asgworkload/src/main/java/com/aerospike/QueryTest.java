package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class QueryTest implements QueryRunnable {

    private final WorkloadProvider provider;

    public QueryTest(WorkloadProvider provider,
                     AGSGraphTraversal ags) {
        this.provider = provider;
        this.provider.setQuery(this);
    }

    @Override
    public String Name() { return "queryTest"; }

    @Override
    public WorkloadTypes WorkloadType() {
        return WorkloadTypes.Test;
    }

    /**
     * @return the Workload description
     */
    @Override
    public String getDescription() {
        return "Test Query";
    }

    @Override
    public WorkloadProvider Start() {
        return provider.Start();
    }

    @Override
    public WorkloadProvider PrintSummary() {
        return provider.PrintSummary();
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

    @Override
    public void close() {
        //No Opt
    }
}

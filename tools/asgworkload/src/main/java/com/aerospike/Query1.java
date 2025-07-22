package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Query1 implements QueryRunnable {

    private final WorkloadProvider provider;
    private final AGSGraphTraversal agsGraphTraversal;
    private final Long scaleFactor;
    private final String vertexIdFmt;

    public Query1(WorkloadProvider provider,
                  AGSGraphTraversal ags) {
        this.provider = provider;
        this.agsGraphTraversal = ags;

        scaleFactor = provider.getCliArgs().vertexScaleFactor;
        vertexIdFmt = provider.getCliArgs().vertexIdFmt;

        this.provider.setQuery(this);
    }

    @Override
    public String Name() { return "Query1"; }

    @Override
    public WorkloadTypes WorkloadType() {
        return WorkloadTypes.Query;
    }

    /**
     */
    @Override
    public String getDescription() {
        return "Query1";
    }

    /**
     * @return true to indicate to execute the workload and false to abort.
     */
    @Override
    public boolean preProcess() {
        return true;
    }

    /**
     *
     */
    @Override
    public void postProcess() {
    }

    private String getRandomId() {
        final Long id = (long) (Math.random() * scaleFactor);
        return String.format(vertexIdFmt, id);
    }

    /**
     * Performs the actual workload
     * @return True to measure the workload and false to indicate the workload was aborted.
     */
    @Override
    public Boolean call()  {
        G().V(getRandomId()).bothE().has("ban", true).otherV().elementMap().toList();
        return true;
    }

    /**
     * @return the AGS Graph Traversal instance
     */
    @Override
    public GraphTraversalSource G() {
        return this.agsGraphTraversal.G();
    }

    /**
     * @return the AGS cluster instance
     */
    @Override
    public Cluster getCluster() {
        return this.agsGraphTraversal.getCluster();
    }

    @Override
    public void close() {
        //No Opt
    }
}

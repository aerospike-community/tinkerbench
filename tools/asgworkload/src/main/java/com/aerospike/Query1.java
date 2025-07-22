package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class Query1 implements QueryRunnable {

    private WorkloadProvider provider;

    /**
     */
    @Override
    public void setWorkloadProvider(WorkloadProvider provider) {
        this.provider = provider;
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

    private GraphTraversalSource g = null;
    private Long scaleFactor = -1L;
    private String vertexIdFmt = null;

    /**
     */
    @Override
    public boolean preProcess() {

        final Cluster.Builder clusterBuilder = Cluster.build();
        clusterBuilder.port(provider.getCliArgs().port).enableSsl(false);
        for (String host : provider.getCliArgs().agsHosts) {
            clusterBuilder.addContactPoint(host);
        }
        final Cluster cluster = clusterBuilder.create();
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        if (provider.getCliArgs().parallelize > 0) {
            g = g.with("aerospike.graph.parallelize",
                        provider.getCliArgs().parallelize);
        }
        scaleFactor = provider.getCliArgs().vertexScaleFactor;
        vertexIdFmt = provider.getCliArgs().vertexIdFmt;

        System.out.println("PreProcess Query1");
        System.out.println(provider);

        return true;
    }

    /**
     *
     */
    @Override
    public void postProcess() {
        System.out.println("PostProcess Query1");
        System.out.println(provider);
    }

    private String getRandomId() {
        final Long id = (long) (Math.random() * scaleFactor);
        return String.format(vertexIdFmt, id);
    }

    /**
     *
     */
    @Override
    public Boolean call() throws InterruptedException {
        g.V(getRandomId()).bothE().has("ban", true).otherV().elementMap().toList();
        return true;
    }
}

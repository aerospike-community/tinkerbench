package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class AGSGraphTraversalSource  implements AGSGraphTraversal {

    private final boolean debug;
    private final Cluster cluster;
    private final GraphTraversalSource g;
    private final OpenTelemetry openTelemetry;
    private final LogSource logger = LogSource.getInstance();

    public AGSGraphTraversalSource(AGSWorkloadArgs args,
                                   OpenTelemetry openTelemetry) {

        this.debug = args.debug;
        this.openTelemetry = openTelemetry;

        logger.PrintDebug("AGS",
                        "Creating Cluster Hosts %s on port %d...%n",
                            String.join(",", args.agsHosts),
                            args.port);

        try {
            final Cluster.Builder clusterBuilder = Cluster.build();
            clusterBuilder.port(args.port).enableSsl(false);
            for (String host : args.agsHosts) {
                clusterBuilder.addContactPoint(host);
            }

            if (args.testMode)
                this.cluster = null;
            else
                this.cluster = clusterBuilder.create();

            logger.PrintDebug("AGS",
                    "Creating GraphTraversalSource...");

            if (args.testMode)
                this.g = null;
            else {
                GraphTraversalSource gdb = traversal().withRemote(DriverRemoteConnection.using(cluster));

                if (args.parallelize > 0) {
                    gdb = gdb.with("aerospike.graph.parallelize",
                            args.parallelize);
                }
                this.g = gdb;
            }
        }
        catch (Exception e) {
            logger.error("Error creating GraphTraversalSource", e);
            throw new RuntimeException(e);
        }

        logger.PrintDebug("AGS", "Creation Done...");
    }

    /**
     * @return The AGS Graph DB instance
     */
    @Override
    public GraphTraversalSource G() {
        return this.g;
    }

    /**
     * @return The AGS Cluster instance
     */
    @Override
    public Cluster getCluster() {
        return this.cluster;
    }

    @Override
    public void close() {

        if(debug)
            System.out.println("DEBUG AGS Closing...");

        try {
            if(this.g != null)
                this.g.close();
            if(this.cluster != null)
                this.cluster.close();
        } catch (Exception e) {
            if(debug)
                System.err.printf("DEBUG AGS Closing error: %s...%n",
                                    e.getMessage());
            openTelemetry.addException(e);
        }
        if(debug)
            System.out.println("DEBUG AGS Closed...");
    }
}

package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.Closeable;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public final class AGSGraphTraversalSource  implements AGSGraphTraversal, Closeable {

    private final Cluster cluster;
    private final GraphTraversalSource g;
    private final OpenTelemetry openTelemetry;
    private final LogSource logger = LogSource.getInstance();

    public AGSGraphTraversalSource(TinkerBench2Args args,
                                   OpenTelemetry openTelemetry) {

        this.openTelemetry = openTelemetry;

        logger.PrintDebug("AGS",
                        "Creating Cluster Hosts %s on port %d..\n",
                            String.join(",", args.agsHosts),
                            args.port);

        try {
            final Cluster.Builder clusterBuilder = args.clusterConfigurationFile == null
                                                    ? Cluster.build()
                                                    : Cluster.build(args.clusterConfigurationFile);

            if( args.clusterConfigurationFile == null) {
                clusterBuilder.port(args.port);

                for (String host : args.agsHosts) {
                    clusterBuilder.addContactPoint(host);
                }
            }

            if (args.appTestMode) {
                this.cluster = null;
                System.err.println();
                System.err.println("Warning: Running in Test Mode!");
                System.err.println("\tNo AGS connection will be attempted!");
                System.err.println();
                logger.warn("Running in Test Mode! No Connection!");
            }
            else
                this.cluster = clusterBuilder.create();

            logger.PrintDebug("AGS",
                    "Creating GraphTraversalSource...");

            if (args.appTestMode)
                this.g = null;
            else {
                GraphTraversalSource gdb = traversal().withRemote(DriverRemoteConnection.using(cluster));

                if(args.gremlinConfigOptions != null) {
                    for(GraphConfigOptions opt : args.gremlinConfigOptions) {
                        gdb = gdb.with(opt.getKey(),
                                        opt.getValue());
                    }
                }

                if(args.asConfigOptions != null) {
                    for(GraphConfigOptions opt : args.asConfigOptions) {
                        gdb = gdb.with(opt.getKey(),
                                opt.getValue());
                    }
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

        logger.PrintDebug("AGS", "Closing GraphTraversalSource...");

        try {
            if(this.g != null)
                this.g.close();
            if(this.cluster != null)
                this.cluster.close();
        } catch (Exception e) {
            logger.error("Error closing GraphTraversalSource", e);
            logger.PrintDebug("AGS", "Error closing GraphTraversalSource", e);

            openTelemetry.addException(e);
        }
        logger.PrintDebug("AGS", "Closed GraphTraversalSource...");
    }
}

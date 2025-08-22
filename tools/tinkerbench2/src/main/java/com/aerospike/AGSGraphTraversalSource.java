package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnectionException;
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
                try {
                    if (g.inject(0).next() != 0) {
                        String hosts = String.join(",", args.agsHosts);
                        logger.error("Connection to Database '{}' at port {} Failed!",
                                hosts,
                                args.port);
                        System.err.printf("Error: Connection to Database '%s' at port %d Failed%n\tAborting Run...%n",
                                hosts,
                                args.port);
                        args.abortRun.set(true);
                    }
                } catch (IllegalStateException e) {
                    if(e.getCause() instanceof RemoteConnectionException) {
                        String msg = String.format("Could not connect to Database '%s' at port %d",
                                String.join(",", args.agsHosts),
                                args.port);
                        logger.error(msg, e);

                        System.err.printf("%s%n\tAborting Run...%n",
                                            msg);
                        args.abortRun.set(true);
                    }
                    else {
                        throw e;
                    }
                }
            }
        }
        catch (Exception e) {
            logger.error("Error creating GraphTraversalSource", e);
            System.err.printf("Error occurred trying to connect to '%s' at port %d%n%n",
                                String.join(",", args.agsHosts),
                                args.port);
            System.err.printf("\tError is %s%n",
                                e.getClass().getSimpleName());
            System.err.printf("\tError Message is %s%n",
                                e.getMessage());
            if(logger.loggingEnabled())
                System.err.println("\tReview log for error details...");
            else
                System.err.println("\tEnable logging for error details...");
            args.abortRun.set(true);
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

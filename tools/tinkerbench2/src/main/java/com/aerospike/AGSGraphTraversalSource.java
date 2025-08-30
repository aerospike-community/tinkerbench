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

            if(args.clusterBuilderOptions != null) {
                for(GraphConfigOptions opt : args.clusterBuilderOptions) {
                    try {
                        logger.PrintDebug("AGS Cluster Builder Option",
                                "Setting %s=%s",
                                opt.getKey(), opt.getValue());
                        Helpers.UpdateFieldSetter(clusterBuilder,
                                                    opt.getKey(),
                                                    opt.getValue(),
                                                    false);
                    } catch (NoSuchMethodException e) {
                        String msg = String.format("Cluster Builder option '%s' (value '%s') does not exist. Please provide a valid option.",
                                                    opt.getKey(),
                                                    opt.getValue());
                        logger.error(msg, e);
                        System.err.printf("%s%n\tAborting Run...%n",
                                msg);
                        openTelemetry.addException(e);
                        args.abortRun.set(true);
                        this.cluster = null;
                        this.g = null;
                        return;
                    } catch (IllegalAccessException e) {
                        String msg = String.format("Cluster Builder option '%s' (value '%s') cannot be accessed. Is this a valid option that can be updated?",
                                opt.getKey(),
                                opt.getValue());
                        logger.error(msg, e);
                        System.err.printf("%s%n\tAborting Run...%n",
                                msg);
                        openTelemetry.addException(e);
                        args.abortRun.set(true);
                        this.cluster = null;
                        this.g = null;
                        return;
                    } catch (IllegalArgumentException e) {
                        String msg = String.format("Cluster Builder option '%s' with value '%s' (type provided %s) is an invalid value or incorrect type. Please provide the correct value or type for this option...",
                                                    opt.getKey(),
                                                    opt.getValue(),
                                                    opt.getValue().getClass().getSimpleName());
                        logger.error(msg, e);
                        System.err.printf("%s%n\tAborting Run...%n",
                                msg);
                        openTelemetry.addException(e);
                        args.abortRun.set(true);
                        this.cluster = null;
                        this.g = null;
                        return;
                    } catch (Exception e) {
                        String msg = String.format("Cluster Builder option '%s' could not be set with value '%s'.%n\tTrying to set this option cased an exception!%n\t\tException Message: %s",
                                                    opt.getKey(),
                                                    opt.getValue(),
                                                    Helpers.getErrorMessage(e));
                        logger.PrintDebug(msg, e);
                        logger.error(msg, e);
                        System.err.printf("%s%n\tAborting Run...%n",
                                            msg);
                        if(logger.loggingEnabled())
                            System.err.println("\tReview log for error details...");
                        else
                            System.err.println("\tEnable logging for error details...");

                        openTelemetry.addException(e);
                        args.abortRun.set(true);
                        this.cluster = null;
                        this.g = null;
                        return;
                    }
                }
            }

            {
                final Cluster tmpCluster;

                if (args.appTestMode) {
                    tmpCluster = null;
                    System.err.println();
                    System.err.println("Warning: Running in Test Mode!");
                    System.err.println("\tNo AGS connection will be attempted!");
                    System.err.println();
                    logger.warn("Running in Test Mode! No Connection!");
                } else {
                    try {
                        tmpCluster = clusterBuilder.create();
                    } catch (IllegalArgumentException e) {
                        logger.error("Error creating GraphTraversalSource", e);
                        if (args.clusterConfigurationFile == null)
                            System.err.printf("Error occurred trying to connect to '%s' at port %d%n",
                                    String.join(",", args.agsHosts),
                                    args.port);
                        else
                            System.err.printf("Error occurred trying to connect using builder file '%s'%n",
                                    args.clusterConfigurationFile);
                        System.err.printf("\tCluster Builder Error:%n\t\t%s%n", e.getMessage());
                        openTelemetry.addException(e);
                        args.abortRun.set(true);
                        this.cluster = null;
                        this.g = null;
                        return;
                    }
                }

                this.cluster = tmpCluster;
            }

            if (args.appTestMode || cluster == null) {
                this.g = null;
            } else {
                logger.PrintDebug("AGS",
                        "Creating GraphTraversalSource...");

                {
                    GraphTraversalSource gdb = traversal().withRemote(DriverRemoteConnection.using(cluster));

                    if (args.gremlinConfigOptions != null) {
                        for (GraphConfigOptions opt : args.gremlinConfigOptions) {
                            logger.PrintDebug("AGS Gremlin Option",
                                    "Set %s=%s",
                                    opt.getKey(), opt.getValue());
                            gdb = gdb.with(opt.getKey(),
                                    opt.getValue());
                        }
                    }

                    if (args.asConfigOptions != null) {
                        for (GraphConfigOptions opt : args.asConfigOptions) {
                            logger.PrintDebug("AGS Aerospike-Gremlin Option",
                                    "Aerospike Setting %s=%s",
                                    opt.getKey(), opt.getValue());
                            gdb = gdb.with(opt.getKey(),
                                    opt.getValue());
                        }
                    }

                    this.g = gdb;
                }
                try {
                    if (g.inject(0).next() == 0) {
                        Helpers.Println(System.out,
                                            "Connected Successfully",
                                            Helpers.BLACK,
                                            Helpers.GREEN_BACKGROUND);
                        logger.info("Connected Successfully");
                    } else {
                        String hosts = String.join(",", args.agsHosts);
                        logger.error("Connection to Database '{}' at port {} Failed!",
                                hosts,
                                args.port);
                        System.err.printf("Error: Connection to Database '%s' at port %d Failed%n\tAborting Run...%n",
                                hosts,
                                args.port);
                        openTelemetry.addException("Connection Failed", "Gremlin inject zero failed");
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
                        openTelemetry.addException((Exception) e.getCause());
                        args.abortRun.set(true);
                    }
                    else {
                        throw e;
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error creating GraphTraversalSource", e);
            if(args.clusterConfigurationFile == null)
                System.err.printf("Error occurred trying to connect to '%s' at port %d%n%n",
                                        String.join(",", args.agsHosts),
                                        args.port);
            else
                System.err.printf("Error occurred trying to connect using builder file '%s'%n%n",
                                    args.clusterConfigurationFile);
            System.err.printf("\tError is %s%n",
                                e.getClass().getSimpleName());
            System.err.printf("\tError Message is %s%n",
                                e.getMessage());
            if(logger.loggingEnabled())
                System.err.println("\tReview log for error details...");
            else
                System.err.println("\tEnable logging for error details...");
            openTelemetry.addException(e);
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

package com.aerospike.tinkerbench.benchmarks;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

import static com.aerospike.tinkerbench.util.BenchmarkUtil.getHost;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMaxConnectionPoolSize;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMaxInProcessPerConnection;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMaxSimultaneousUsagePerConnection;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMinConnectionPoolSize;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMinSimultaneousUsagePerConnection;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getPort;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getSSL;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public abstract class TinkerBench {
    private Cluster cluster = null;
    protected GraphTraversalSource g = null;

    private static final Cluster.Builder BUILDER = Cluster.build()
            .addContactPoints(getHost())
            .port(getPort())
            .maxConnectionPoolSize(getMaxConnectionPoolSize())
            .maxInProcessPerConnection(getMaxInProcessPerConnection())
            .maxSimultaneousUsagePerConnection(getMaxSimultaneousUsagePerConnection())
            .minSimultaneousUsagePerConnection(getMinSimultaneousUsagePerConnection())
            .minConnectionPoolSize(getMinConnectionPoolSize())
            .enableSsl(getSSL());

    @Setup
    public void setupBenchmark(final BenchmarkParams benchmarkParams) {
        System.out.println("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        System.out.println("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    // Tear down for benchmark.
    @TearDown
    public void tearDownBenchmark() {
        if (g != null) {
            try {
                System.out.println("Closing the GraphTraversalSource.");
                g.close();
            } catch (final Exception e) {
                System.out.println("Failed to close the GraphTraversalSource.");
            }
        }
        if (cluster != null) {
            try {
                System.out.println("Closing the Cluster.");
                cluster.close();
            } catch (final Exception e) {
                System.out.println("Failed to close the Cluster.");
            }
        }
    }
}

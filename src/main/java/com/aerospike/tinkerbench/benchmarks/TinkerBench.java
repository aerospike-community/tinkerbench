package com.aerospike.tinkerbench.benchmarks;

import com.aerospike.tinkerbench.util.BenchmarkUtil;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.tinkerbench.benchmarks.simple_bench.BenchmarkSimple.RANDOM;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.*;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMaxSimultaneousUsagePerConnection;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMinConnectionPoolSize;
import static com.aerospike.tinkerbench.util.BenchmarkUtil.getMinSimultaneousUsagePerConnection;

public abstract class TinkerBench {
    private Cluster cluster = null;
    protected List<GraphTraversalSource> g = new ArrayList<>();
    private static final Cluster.Builder BUILDER;
    static {
        BUILDER = Cluster.build()
                .addContactPoints(getHost())
                .port(getPort())
                .maxConnectionPoolSize(getMaxConnectionPoolSize())
                .maxInProcessPerConnection(getMaxInProcessPerConnection())
                .maxSimultaneousUsagePerConnection(getMaxSimultaneousUsagePerConnection())
                .minSimultaneousUsagePerConnection(getMinSimultaneousUsagePerConnection())
                .minConnectionPoolSize(getMinConnectionPoolSize())
                .enableSsl(getSSL());
        String username = BenchmarkUtil.getUser();
        String password = BenchmarkUtil.getPassword();
        if (username != null && password != null) {
            BUILDER.credentials(username, password);
        } else if (username != null || password != null) {
            throw new IllegalArgumentException("No username or password provided. To use auth provide both username and password.");
        }
    }
    @Setup
    public void setupBenchmark(final BenchmarkParams benchmarkParams) {
        System.out.println("Creating the Cluster (setup).");

        System.out.println("Creating the GraphTraversalSource (setup).");
        cluster = BUILDER.create();
        for (int i = 0; i < getClientCount(); i++) {
            System.out.println("Creating client #" + i);
            final Client client = cluster.connect();
            g.add(traversal().withRemote(DriverRemoteConnection.using(client)));
        }
    }

    public GraphTraversalSource getRandomGraphTraversalSource() {
        return g.get(RANDOM.nextInt(g.size()));
    }

    // Tear down for benchmark.
    @TearDown
    public void tearDownBenchmark() {
        for (int i = 0; i < g.size(); i++) {
            if (g.get(i) != null) {
                try {
                    System.out.println("Closing the GraphTraversalSource.");
                    g.get(i).close();
                    g.set(i, null);
                } catch (final Exception e) {
                    System.out.println("Failed to close the GraphTraversalSource.");
                }
            }
        }
        g.clear();
        if (cluster != null) {
            try {
                System.out.println("Closing the Cluster (teardown).");
                cluster.close();
                cluster = null;
            } catch (final Exception e) {
                System.out.println("Failed to close the Cluster (teardown).");
            }
        }
    }
}

package com.aerospike.tinkerbench;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

@BenchmarkMode({Mode.Throughput})
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
// Takes about 30 minutes to run in GitHub actions.
@Measurement(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
public class BenchmarkInsertion {
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkInsertion.class);
    private static final String LOCALHOST = "localhost";
    public static final String DEFAULT_PORT = "8182";
    private static final String HOST = getHost();
    private static final int PORT = getPort();
    private Cluster cluster = null;
    private GraphTraversalSource g = null;
    private int connectionPoolSize = 8;

    public static String envPropDefault(final String key, final String def) {
        Optional<String> envVal = Optional.ofNullable(System.getenv(key));
        Optional<String> propVal = Optional.ofNullable(System.getProperty(key));
        if (envVal.isPresent())
            return envVal.get();
        if (propVal.isPresent())
            return propVal.get();
        throw new RuntimeException("Provide an environment variable or Java system property for " + key);
    }

    public static String getHost() {
        return envPropDefault("HOST", LOCALHOST);
    }

    public static int getPort() {
        return Integer.parseInt(envPropDefault("PORT", DEFAULT_PORT));
    }

    private static final Cluster.Builder BUILDER = Cluster.build()
            .addContactPoint(HOST)
            .port(PORT)
            .maxConnectionPoolSize(8)
            .enableSsl(false);
    private static final Map<String, String> testToTraversal = Map.ofEntries(
            Map.entry("benchmark_g_addV", "g.addV()")
    );

    @Setup
    public void setup(final BenchmarkParams benchmarkParams) {
        LOG.info("Creating the Cluster (setup).");
        cluster = BUILDER.create();

        LOG.info("Creating the GraphTraversalSource (setup).");
        g = traversal().withRemote(DriverRemoteConnection.using(cluster));
    }

    // Teardown for benchmark.
    @TearDown
    public void tearDown() {
        if (g != null) {
            try {
                LOG.info("Closing the GraphTraversalSource.");
                g.close();
            } catch (Exception e) {
                LOG.info("Failed to close the GraphTraversalSource.");
            }
        }
        if (cluster != null) {
            try {
                LOG.info("Closing the Cluster.");
                cluster.close();
            } catch (Exception e) {
                LOG.info("Failed to close the Cluster.");
            }
        }
    }

    // Use junit test to hook into maven nicely. This test will
    // use the benchmark framework which runs the benchmarks in a
    // separate JVM.
    public static void benchmark() throws RunnerException {
        final ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(BenchmarkInsertion.class.getSimpleName())
                .detectJvmArgs()
                .forks(2)
                .measurementIterations(2)
                .measurementTime(TimeValue.seconds(30))
                .timeout(TimeValue.minutes(1)); // Timeout
        Options opt = optBuilder.build();
        Collection<RunResult> runResult = new Runner(opt).run();


        final List<Map<String, Object>> root = new ArrayList<>();
        runResult.forEach(result -> {
            Map<String, Object> obj = new HashMap<>();
            obj.put("name", testToTraversal.get(result.getPrimaryResult().getLabel()));
            obj.put("unit", result.getPrimaryResult().getScoreUnit());
            obj.put("value", result.getPrimaryResult().getScore());
            obj.put("range", result.getPrimaryResult().getScoreError());
            obj.put("extra", "\n\t" + result.getPrimaryResult().getStatistics());
            root.add(obj);
        });
        root.forEach(resultMap -> {
            System.out.println("result: ");
            resultMap.forEach((k, v) -> {
                System.out.printf("\t %s:  %s", k, v);
                System.out.println();
            });
        });
    }

    // Remove person data as it is added.
    @Setup(Level.Iteration)
    public void setupInvocation() {
        g.V().drop().iterate();
    }

//    @Benchmark
//    public void benchmark_g_addV_addE_addV(final Blackhole blackhole) {
//        Queries.insertVVE(g);
//    }

    @Benchmark
    public void benchmark_coalesce(final Blackhole blackhole) {
        Queries.coalesce(g);
    }
}

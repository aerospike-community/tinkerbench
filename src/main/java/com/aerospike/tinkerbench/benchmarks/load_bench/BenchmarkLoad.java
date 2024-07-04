package com.aerospike.tinkerbench.benchmarks.load_bench;

import com.aerospike.tinkerbench.benchmarks.TinkerBench;
import com.aerospike.tinkerbench.util.BenchmarkUtil;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 0)
public class BenchmarkLoad extends TinkerBench {

    public static final int SEED_COUNT = BenchmarkUtil.getBenchmarkSeedSize();
    public static final int SEED_COUNT_MULTIPLIER = BenchmarkUtil.getBenchmarkSeedRuntimeMultiplier();
    public static final Random RANDOM = new Random();

    @Setup
    public void setupForBenchmark(final BenchmarkParams benchmarkParams) {
        System.out.println("Performing initial seeding for benchmark.");
        g.V().drop().iterate();
        BenchmarkUtil.seedGraph(g, SEED_COUNT);
        System.out.println("Completed initial seeding for benchmark.");
    }

    @Benchmark
    public void benchmarkGetOrCreateVertex() {
        final int randomSeed = RANDOM.nextInt(SEED_COUNT * SEED_COUNT_MULTIPLIER);
        getOrCreateVertex(randomSeed);
    }

    private Vertex getOrCreateVertex(final int seed) {
        try {
            return g.mergeV(Map.of(T.id, seed)).
                    option(Merge.onCreate,
                            Map.of(T.label, "vertex_label", "property_key", "property_value")).next();
        } catch (final Exception e) {
            if (e.getMessage().contains("Vertex with id already exists")) {
                return g.V(seed).next();
            } else {
                throw e;
            }
        }
    }

    @Benchmark
    public void benchmarkGetOrCreateEdge() {
        final Vertex v1 = getOrCreateVertex(RANDOM.nextInt(SEED_COUNT * SEED_COUNT_MULTIPLIER));
        final Vertex v2 = getOrCreateVertex(RANDOM.nextInt(SEED_COUNT * SEED_COUNT_MULTIPLIER));
        g.addE("edge_label").from(v1).to(v2).property("property_key", "property_value").iterate();
    }

    @Benchmark
    public void benchmarkGetVertex() {
        final Vertex v1 = getOrCreateVertex(RANDOM.nextInt(SEED_COUNT));
        g.V(v1).next();
    }

    @Benchmark
    public void benchmarkTraversal() {
        final Vertex v1 = getOrCreateVertex(RANDOM.nextInt(SEED_COUNT));
        g.V(v1).both().both().both().both().toList();
    }
}

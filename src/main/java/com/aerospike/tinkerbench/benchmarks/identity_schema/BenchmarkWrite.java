package com.aerospike.tinkerbench.benchmarks.identity_schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
public class BenchmarkWrite extends BenchmarkIdentitySchema {

    @Benchmark
    public void crossStitchOneHouseholdGivenTwoGoldenEntity(final Blackhole blackhole) {
        // SR1: Find all Devices Used by a User using a device :
        //      Retrieve devices associated with a given user, which is a fundamental query for cross-device targeting.
        final Object goldenEntity1 = getGoldenEntity();
        final Object goldenEntity2 = getGoldenEntity();
        blackhole.consume(g.V(goldenEntity1).out("HAS_HOUSEHOLD").limit(1).
                addE("HAS_HOUSEHOLD").from(__.V(goldenEntity2)).
                toList());
    }
}

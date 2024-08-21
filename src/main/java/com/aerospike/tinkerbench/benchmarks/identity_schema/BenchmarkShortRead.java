package com.aerospike.tinkerbench.benchmarks.identity_schema;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
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
public class BenchmarkShortRead extends BenchmarkIdentitySchema {

    @Benchmark
    public void SR1_findAllDevicesGivenInputDevice(final Blackhole blackhole) {
        // SR1: Find all Devices Used by a User using a device :
        //      Retrieve devices associated with a given user, which is a fundamental query for cross-device targeting.
        blackhole.consume(
                g.V(getDeviceId()).
                        in("HAS_DEVICE").
                        repeat(__.out()).
                        until(__.hasLabel("Device")).toList()
        );
    }

    @Benchmark
    public void SR2_listAllSignalsLinkedGivenInputDevice(final Blackhole blackhole) {
        // SR2: List All signals Linked to a Device:
        //      Identifies all tracking identifiers (cookies, device fingerprints) associated with a device,
        //      critical for user identification across partners.
        blackhole.consume(
                g.V(getDeviceId()).
                        in("HAS_DEVICE","PROVIDED_DEVICE").
                        repeat(__.out()).
                        until(__.not(
                                __.hasLabel("Partner","Household"))).
                        dedup()
        );
    }

    @Benchmark
    public void SR3_listAllSignalsLinkedGivenInputGoldenEntity(final Blackhole blackhole) {
        // SR3: List All signals Linked to a Golden Entity:
        //      Identifies all tracking identifiers (cookies, device fingerprints) associated with a Golden Entity,
        //      critical for user identification across partners.
        blackhole.consume(
                g.V(getGoldenEntity()).
                        out("HAS_PARTNER").out().toList()
        );
    }

    @Benchmark
    public void SR4_listAllSignalsLinkedGivenInputGoldenEntityAndPartnerType(final Blackhole blackhole) {
        // SR4: Start with GoldenEntity and get all signals that have been provided by a specific partner
        blackhole.consume(
                g.V(getGoldenEntity()).
                        out("HAS_PARTNER").
                        has("type", getRandomPartnerName()).out()
        );
    }

    @Benchmark
    public void SR5_listAllPartnerTypesGivenInputGoldenEntity(final Blackhole blackhole) {
        // SR5: GoldenEntity is known information: lookup by GoldenEntity id all Partner ids and Names
        blackhole.consume(
                g.V(getGoldenEntity()).
                out("HAS_PARTNER").
                elementMap("type").toList()
        );
    }
}

package com.aerospike.tinkerbench.benchmarks.identity_schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Instant;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
public class BenchmarkShortRead extends BenchmarkIdentitySchema {
    private static final Random RANDOM = new Random();

    @Benchmark
    public void SR1_findAllDevicesGivenInputDevice(final Blackhole blackhole) {
        // SR1: Find all Devices Used by a User using a device :
        //      Retrieve devices associated with a given user, which is a fundamental query for cross-device targeting.
        if (RANDOM.nextInt(10) == 1) {
            Instant instant = Instant.now();
            TraversalMetrics tm = g.V(getDeviceId()).
                    in("HAS_DEVICE").
                    repeat(__.out()).
                    until(__.hasLabel("Device")).profile().next();
            Instant instant2 = Instant.now();
            System.out.println("SR1 Profile " + instant2.minusMillis(instant.toEpochMilli()).toEpochMilli() + "\n" + tm);
        } else {
            blackhole.consume(
                    g.V(getDeviceId()).
                            in("HAS_DEVICE").
                            repeat(__.out()).
                            until(__.hasLabel("Device")).toList());
        }
    }

    @Benchmark
    public void SR2_listAllSignalsLinkedGivenInputDevice(final Blackhole blackhole) {
        // SR2: List All signals Linked to a Device:
        //      Identifies all tracking identifiers (cookies, device fingerprints) associated with a device,
        //      critical for user identification across partners.
        if (RANDOM.nextInt(10) == 1) {
            Instant instant = Instant.now();
            TraversalMetrics tm = g.V(getDeviceId()).
                    in("HAS_DEVICE", "PROVIDED_DEVICE").
                    repeat(__.out()).
                    until(__.not(
                            __.hasLabel("Partner", "Household"))).
                    dedup().profile().next();
            Instant instant2 = Instant.now();
            System.out.println("SR2 Profile " + instant2.minusMillis(instant.toEpochMilli()).toEpochMilli() + "\n" + tm);
        } else {
            blackhole.consume(
                    g.V(getDeviceId()).
                            in("HAS_DEVICE", "PROVIDED_DEVICE").
                            repeat(__.out()).
                            until(__.not(
                                    __.hasLabel("Partner", "Household"))).
                            dedup().toList()
            );
        }
    }

    @Benchmark
    public void SR3_listAllSignalsLinkedGivenInputGoldenEntity(final Blackhole blackhole) {
        // SR3: List All signals Linked to a Golden Entity:
        //      Identifies all tracking identifiers (cookies, device fingerprints) associated with a Golden Entity,
        //      critical for user identification across partners.
        if (RANDOM.nextInt(10) == 1) {
            Instant instant = Instant.now();
            TraversalMetrics tm = g.V(getGoldenEntity()).
                    out("HAS_PARTNER").out().profile().next();
            Instant instant2 = Instant.now();
            System.out.println("SR3 Profile " + instant2.minusMillis(instant.toEpochMilli()).toEpochMilli() + "\n" + tm);
        } else {
            blackhole.consume(
                    g.V(getGoldenEntity()).
                            out("HAS_PARTNER").out().toList()
            );
        }
    }

    @Benchmark
    public void SR4_listAllSignalsLinkedGivenInputGoldenEntityAndPartnerType(final Blackhole blackhole) {
        // SR4: Start with GoldenEntity and get all signals that have been provided by a specific partner
        if (RANDOM.nextInt(10) == 1) {
            Instant instant = Instant.now();
            TraversalMetrics tm = g.V(getGoldenEntity()).
                    out("HAS_PARTNER").
                    has("type", getRandomPartnerName()).out().profile().next();
            Instant instant2 = Instant.now();
            System.out.println("SR4 Profile " + instant2.minusMillis(instant.toEpochMilli()).toEpochMilli() + "\n" + tm);
        } else {
            blackhole.consume(
                    g.V(getGoldenEntity()).
                            out("HAS_PARTNER").
                            has("type", getRandomPartnerName()).out().toList()
            );
        }
    }

    @Benchmark
    public void SR5_listAllPartnerTypesGivenInputGoldenEntity(final Blackhole blackhole) {
        // SR5: GoldenEntity is known information: lookup by GoldenEntity id all Partner ids and Names
        if (RANDOM.nextInt(10) == 1) {
            Instant instant = Instant.now();
            TraversalMetrics tm = g.V(getGoldenEntity()).
                    out("HAS_PARTNER").
                    elementMap("type").profile().next();
            Instant instant2 = Instant.now();
            System.out.println("SR5 Profile " + instant2.minusMillis(instant.toEpochMilli()).toEpochMilli() + "\n" + tm);
        } else {
            blackhole.consume(
                    g.V(getGoldenEntity()).
                            out("HAS_PARTNER").
                            elementMap("type").toList()
            );
        }
    }
}

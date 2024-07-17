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
    public void finalAllDevicesUsedByUserUsingDevice(final Blackhole blackhole) {
        //SR1: Find all Devices Used by a User using a device
        blackhole.consume(g.V(getDeviceId()).in("HAS_DEVICE").out("HAS_DEVICE").toList());
    }

    @Benchmark
    public void identifyUsersAssociatedWithSpecificDevice(final Blackhole blackhole) {
        //SR2: Identify Users Associated with a Specific Device
        blackhole.consume(g.V(getDeviceId()).in("HAS_DEVICE").toList());
    }

    @Benchmark
    public void listAllSignalsLinkedToADevice(final Blackhole blackhole) {
        //SR3: List All signals Linked to a Device
        blackhole.consume(g.V(getDeviceId()).in("HAS_DEVICE").out().not(__.hasLabel("PartnerIdentity")).toList());
    }

    @Benchmark
    public void getAllSignalsAssociatedWithGoldenEntity(final Blackhole blackhole) {
        //SR4: Start on GoldenEntity and get all signals that have been provided by all partners
        blackhole.consume(g.V(getGoldenEntity()).out().not(__.hasLabel("PartnerIdentity")).not(__.hasLabel("Household")).toList());
    }

    @Benchmark
    public void getAllSignalsProvidedByAPartner(final Blackhole blackhole) {
        //SR5: GoldenEntity and get all signals that have been provided by a specific  partner
        blackhole.consume(g.V(getGoldenEntity()).out().where(
                        __.in().has("PartnerIdentity", "PartnerName", getRandomPartnerName())).toList());
    }

    @Benchmark
    public void getAllGoldenEntities(final Blackhole blackhole) {
        //SR6:lookup by GoldenEntity id and return all PartnerIdentity ids linked to the signals for the goldenEntity
        blackhole.consume(g.V(getGoldenEntity()).out().not(__.hasLabel("PartnerIdentity")).
                in().hasLabel("PartnerIdentity").toList());
    }

    @Benchmark
    public void getAllPartnerIdentitiesFromIpAddress(final Blackhole blackhole) {
        //SR7: Start from Ip address, find PartnerIdentity ids
        blackhole.consume(g.V(getIpAddress()).in("HAS_IP_ADDR").outE("HAS_IDENTITY").
                order().by("score", Order.desc).limit(1).otherV().toList());
    }

    @Benchmark
    public void getIpWithMostPartnersFromIpAddress(final Blackhole blackhole) {
        //SR8: Start from IpAddress, find PartnerIdenties of the GoldenIdentity associated with the IpAddress that has most associated PartnerIdentities
        blackhole.consume(g.V(getIpAddress()).order().by(__.in("HAS_IP_ADDR").out("HAS_IDENTITY").count(), Order.desc).limit(1).toList());
    }
}

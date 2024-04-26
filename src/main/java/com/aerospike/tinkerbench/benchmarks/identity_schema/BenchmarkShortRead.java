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
        blackhole.consume(g.V(getDeviceId()).in("HAS_DEVICE").out("HAS_DEVICE").toList());
    }

    @Benchmark
    public void identifyUsersAssociatedWithSpecificDevice(final Blackhole blackhole) {
        blackhole.consume(g.V(getDeviceId()).in("HAS_DEVICE").toList());
    }

    @Benchmark
    public void listAllSignalsLinkedToADevice(final Blackhole blackhole) {
        blackhole.consume(g.V(getDeviceId()).in("HAS_DEVICE").out().not(__.hasLabel("PartnerIdentity")).toList());
    }

    @Benchmark
    public void getAllSignalsAssociatedWithPartner(final Blackhole blackhole) {
        blackhole.consume(g.V(getPartnerIdentity()).out().toList());
    }

    @Benchmark
    public void getAllSignalsProvidedByAPartner(final Blackhole blackhole) {
        blackhole.consume(g.V(getGoldenEntity()).out().where(
                        __.in().has("PartnerIdentity", "PartnerName", "")).toList());
    }

    @Benchmark
    public void getAllGoldenEntities(final Blackhole blackhole) {
        blackhole.consume(g.V(getGoldenEntity()).out().not(__.hasLabel("PartnerIdentity")).
                in().hasLabel("PartnerIdentity").toList());
    }

    @Benchmark
    public void getAllPartnerIdentitiesFromIpAddress(final Blackhole blackhole) {
        blackhole.consume(g.V(getIpAddress()).in("HAS_IP_ADDR").outE("HAS_IDENTITY").
                order().by("score", Order.desc).limit(1).otherV().toList());
    }

    @Benchmark
    public void getIpWithMostPartnersFromIpAddress(final Blackhole blackhole) {
        blackhole.consume(g.V(getIpAddress()).order().by(__.in("HAS_IP_ADDR").out("HAS_IDENTITY").count(), Order.desc).limit(1).toList());
    }
}

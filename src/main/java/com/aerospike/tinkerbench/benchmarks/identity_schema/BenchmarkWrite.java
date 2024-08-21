package com.aerospike.tinkerbench.benchmarks.identity_schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;
import java.util.List;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
public class BenchmarkWrite extends BenchmarkIdentitySchema {

    @Benchmark
    public void SW1_crossStitchOneHouseholdGivenTwoGoldenEntity(final Blackhole blackhole) {
        // SW1: Find all Devices Used by a User using a device :
        //      Retrieve devices associated with a given user, which is a fundamental query for cross-device targeting.
        final Object goldenEntity1 = getGoldenEntity();
        final Object goldenEntity2 = getGoldenEntity();
        blackhole.consume(g.V(goldenEntity1).out("HAS_HOUSEHOLD").limit(1).
                addE("HAS_HOUSEHOLD").from(__.V(goldenEntity2)).
                toList());
    }

    @Benchmark
    public void SW2_addNewPartnerToExistingDevice(final Blackhole blackhole) {
        // SW2: Given existing device and new partner/id/cookie,
        //      Create partner subgraph, link partner to golden entity attached to device.
        final Object goldenEntity = getGoldenEntity();
        final List<Object> deviceIds = g.V(goldenEntity).out("HAS_DEVICE").limit(1).id().toList();
        if (deviceIds.isEmpty()) {
            return;
        }
        final Object deviceId = deviceIds.get(0);
        final String partnerName = getRandomPartnerName();
        blackhole.consume(g.V(deviceId).as("device").
                addV("Partner").property("type", partnerName).as("partner").
                addV("Cookie").
                property("domain_hash", "FOO").property("expiration_ts", 1).
                property("last_updated_ts", 1).property("size", 1).
                as("cookie").
                addV("IpAddress").
                property("addresss", "192.168.0.1").
                property("last_updated_ts", 1).
                as("ip").
                addE("PROVIDED_COOKIE").from("partner").to("cookie").
                addE("PROVIDED_IP_ADDRESS").from("partner").to("ip").
                addE("PROVIDED_DEVICE").from("partner").to("device").
                V(deviceId).in("HAS_DEVICE").limit(1).addE("HAS_PARTNER").to("partner").
                toList());
    }

    @Benchmark
    public void SW3_updateTimestampOfEntirePartnerSubgraph(final Blackhole blackhole) {
        // SW3: Partner vertex, update entire partner subgraph last_updated_ts.
        final Object partnerId = getPartnerIdentity();
        blackhole.consume(g.V(partnerId).out().property("last_update_ts", RANDOM.nextLong()).toList());
    }

    private String getSaltString() {
        final String SALTCHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
        final StringBuilder salt = new StringBuilder();
        while (salt.length() < 18) { // length of the random string.
            int index = (int) (RANDOM.nextFloat() * SALTCHARS.length());
            salt.append(SALTCHARS.charAt(index));
        }
        final String saltStr = salt.toString();
        return saltStr;
    }

    @Benchmark
    public void SW4_updateGoldenEntityProperties(final Blackhole blackhole) {
        // SW4: Golden Entity vertex, update it's last updated timestamp.
        final Object goldenEntityId = getGoldenEntity();
        final String randomString = getSaltString();
        blackhole.consume(g.V(goldenEntityId).
                        property("date_of_birth_day", RANDOM.nextInt(31)).
                        property("date_of_birth_month", RANDOM.nextInt(12)).
                        property("date_of_birth_year", RANDOM.nextInt(2024)).
                        property("demonym", randomString).
                        property("educationalAttainment", randomString).
                        property("email_hash", randomString).
                        property("first_name", randomString).
                        property("last_name", randomString).
                        property("maritalStatus", randomString).
                        property("race", randomString).
                        property("sex", randomString).
               toList());
    }

    @Benchmark
    public void SW5_updateScorePropertyOfPartnerOfTypeGivenGoldenEntity(final Blackhole blackhole) {
        // SW5: Partner vertex, update it's score property based on golden entity.
        final Object goldenEntityId = getGoldenEntity();
        blackhole.consume(g.V(goldenEntityId).outE("HAS_PARTNER").
                where(__.otherV().values("type").is(getRandomPartnerName())).
                property("score", RANDOM.nextInt(100)).toList());
    }
}

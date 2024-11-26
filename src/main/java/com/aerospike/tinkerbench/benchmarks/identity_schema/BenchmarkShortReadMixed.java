package com.aerospike.tinkerbench.benchmarks.identity_schema;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.concurrent.TimeUnit;

@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
@Warmup(iterations = 1)
public class BenchmarkShortReadMixed extends BenchmarkIdentitySchema {

    @Param({"-1"})
    public int queryID;

    @Benchmark
    public void randomShortQueryGenerator(final Blackhole blackhole) throws Exception {

        int qID;
        if (queryID == -1) {
            qID = getRandomQueryId(5) + 1;
        } else {
            qID = queryID;
        }

        switch (qID) {
            case 1:
                // SR1: Find all Devices Used by a User using a device:
                //      Retrieve devices associated with a given user, which is a fundamental query for cross-device targeting.
                blackhole.consume(g.V(getDeviceId()).
                        // Go in from Device to Partner or GoldenEntity.
                                in("HAS_DEVICE", "PROVIDED_DEVICE").
                        // Go out from Partner to Device or in from Partner to GoldenEntity.
                        // Go in from GoldenEntity to Partner or out from GoldenEntity to Device.
                                both("HAS_PARTNER", "HAS_DEVICE", "PROVIDED_DEVICE").
                        // If on Partner or GoldenEntity, go out to Device, else no-op.
                                choose(__.hasLabel("Partner", "GoldenEntity"), __.out("PROVIDED_DEVICE", "HAS_DEVICE")).
                        // Remove duplicates.
                                dedup().toList()
                );
                break;

            case 2:
                // SR2: List All signals Linked to a Device:
                //      Identifies all tracking identifiers (cookies, device fingerprints) associated with a device,
                //      critical for user identification across partners.
                blackhole.consume(
                        g.V(getDeviceId()).in("HAS_DEVICE", "PROVIDED_DEVICE").
                                choose(
                                        __.hasLabel("GoldenEntity"),
                                        // GoldenEntity must go to Partners and signals. If on Partner, go out again.
                                        __.out().choose(__.hasLabel("Partner"), __.out()),
                                        // Partner must go to signals and GoldenEntity. If on GoldenEntity, must go back to Partner and then to signals.
                                        __.both().choose(
                                                __.hasLabel("GoldenEntity"),
                                                // GoldenEntity must go to Partners and signals. If on Partner, go out again.
                                                __.out().choose(__.hasLabel("Partner"), __.out())
                                                // Filter out Household.
                                        )).not(__.hasLabel("Household")).toList()
                );
                break;

            case 3:

                // SR3: List All signals Linked to a Golden Entity:
                //      Identifies all tracking identifiers (cookies, device fingerprints) associated with a Golden Entity,
                //      critical for user identification across partners.
                blackhole.consume(
                        g.V(getGoldenEntity()).
                                // Go in and out from GoldenEntity to Partner / signals.
                                        both().
                                // If on partner, go out to partner's signals, else no-op.
                                        choose(__.hasLabel("Partner"), __.out()).
                                // Remove duplicates.
                                        dedup().toList()
                );
                break;

            case 4:
                // SR4: Start with GoldenEntity and get all signals that have been provided by a specific partner
                blackhole.consume(
                        g.V(getGoldenEntity()).
                                // Go out to all Partners.
                                        out("HAS_PARTNER").
                                // Filter Partner on a random partner name.
                                        has("type", getRandomPartnerName()).
                                // Get all signals for the Partner.
                                        out().toList()
                );
                break;

            case 5:
                // SR5: GoldenEntity is known information: lookup by GoldenEntity id all Partner ids and Names
                blackhole.consume(
                        g.V(getGoldenEntity()).
                                // Go out to all partners.
                                        out("HAS_PARTNER").
                                // Return the Partner type.
                                        elementMap("type").toList()
                );
                break;

            default:
                throw new Exception("Invalid query " + qID);
        }
    }
}

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
    }

    @Benchmark
    public void SR2_listAllSignalsLinkedGivenInputDevice(final Blackhole blackhole) {
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
                                )).not(__.hasLabel("Household"))
        );
    }

    @Benchmark
    public void SR3_listAllSignalsLinkedGivenInputGoldenEntity(final Blackhole blackhole) {
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
                        dedup().toList());

                // ==>v[2048593536]
                //==>v[3614674703]
                //==>v[445536236]
                //==>v[1714951721]
                //==>v[2989815197]
                //==>v[1299705913]
                //==>v[3150528505]
                //==>v[3291001900]
                //==>v[3532318076]
                //==>v[1786273760]
                //==>v[3167082517]
                //==>v[2321256277]
                //==>v[2929912535]
                //==>v[2307097553]
                //==>v[3203079830]
                //==>v[3309959396]
                //==>v[1384123894]
                //==>v[2357600251]
                //==>v[3660855620]
                //==>v[1485884483]
                //==>v[3697216053]
                //==>v[3371926694]
                //==>v[694667833]
                //==>v[2747194776]
                //==>v[2995455040]
                //==>v[1821024004]
                //==>v[1446796855]
                //==>v[2608096207]
                //==>v[3268241901]
                //==>v[1479260112]
                //==>v[3226504488]
                //==>v[1857574977]
                //==>v[3881555267]
                //==>v[2652646547]
                //==>v[2751342496]
                //==>v[1894569778]
                //==>v[2123242756]
                //==>v[2264906560]
                //==>v[574794829]
                //==>v[918152983]
                //==>v[833496252]
                //==>v[1445095124]
                //==>v[3885227180]
                //==>v[1789120705]
                //==>v[164864270]
                //==>v[3292343975]
                //==>v[2269878738]
                //==>v[2378488664]
                //==>v[2517172180]
                //==>v[2061828441]
                //==>v[1370757509]
                //==>v[1073354542]
                //==>v[1932829682]
                //==>v[3772565796]
                //==>v[2951972792]
                //==>v[947725109]
                //==>v[627321957]
                //==>v[1816660150]
                //==>v[2815702842]
                //==>v[772762292]
                //==>v[2030239339]
                //==>v[3674963072]
                //==>v[908304032]
                //==>v[2169622261]
                //==>v[2204663999]
                //==>v[1592453571]
                //==>v[168727770]
                //==>v[2163450682]
                //==>v[1298177832]
                //==>v[2249488338]
                //==>v[412116491]
                //==>v[1055337066]
                //==>v[1316031657]
                //==>v[1058310441]
                //==>v[3093900682]
                //==>v[289739266]
                //==>v[490458725]
                //==>v[3090703923]
                //==>v[130543251]
                //==>v[977781493]
                //==>v[1176571319]
                //==>v[1464890747]
                //==>v[2576755200]
                //==>v[3181202171]


                //==>v[1833443273]
                //==>v[3105406224]
                //==>v[104624366]
                //==>v[349722934]
                //==>v[217274234]
                //==>v[1881997677]
                //==>v[1181560430]
                //==>v[3751189101]
                //==>v[1285371094]
                //==>v[2019791182]
                //==>v[1046980476]
                //==>v[725826840]
                //==>v[1276488750]
                //==>v[1951713186]
                //==>v[2053193894]
                //==>v[472984201]
                //==>v[1689226671]
                //==>v[1846129778]
                //==>v[672901979]
                //==>v[1372745597]
                //==>v[1102343737]
                //==>v[1879247667]
                //==>v[440230861]
                //==>v[2997817041]
                //==>v[2629252676]
                //==>v[450726733]
                //==>v[552191849]
                //==>v[3031744907]
                //==>v[3143957217]
                //==>v[2131854799]
                //==>v[2613057817]
                //==>v[3867328174]
                //==>v[1947108913]
                //==>v[2790264264]
                //==>v[3300645626]
                //==>v[2561532703]
                //==>v[1708069268]
                //==>v[3400492511]
                //==>v[3355030552]
                //==>v[3803725564]
                //==>v[3860325415]
                //==>v[1426595460]
                //==>v[421662468]
                //==>v[2463493678]
                //==>v[2420233878]
                //==>v[1541949358]
                //==>v[2606281367]
                //==>v[3640920408]
                //==>v[347682754]
                //==>v[2405102106]
                //==>v[2541824997]
                //==>v[1094034227]
                //==>v[3573395760]
                //==>v[2384338232]
                //==>v[527364297]
                //==>v[3046469261]
                //==>v[2171914603]
                //==>v[3179262966]
                //==>v[1273327199]
                //==>v[2547395596]
                //==>v[2893675822]
                //==>v[743433446]
                //==>v[3769351219]
                //==>v[2661838132]
                //==>v[3517967216]
                //==>v[2255857651]
                //==>v[2978661181]
                //==>v[1034051798]
                //==>v[1354202002]
                //==>v[659762518]
                //==>v[2793771644]
                //==>v[3628546575]
                //==>v[2539528202]
                //==>v[1531519613]
                //==>v[3033406711]
                //==>v[2558360162]
                //==>v[3160936030]
                //==>v[1470562761]
                //==>v[3877012838]
                //==>v[1235587415]
                //==>v[1744091079]
                //==>v[3340143303]
                //==>v[1464223907]
                //==>v[3776515908]
                //==>v[3631103042]
                //==>v[3071405610]
                //==>v[2188619546]
                //==>v[746682076]
                //==>v[900146059]
                //==>v[2519012197]
                //==>v[511214810]
                //==>v[2577459162]
                //==>v[3117848974]
                //==>v[1636387522]
                //==>v[3478276641]
                //==>v[645139644]
                //==>v[3087426550]
                //==>v[1677227156]
                //==>v[1940145377]
                //==>v[3503846007]
        );
    }

    @Benchmark
    public void SR4_listAllSignalsLinkedGivenInputGoldenEntityAndPartnerType(final Blackhole blackhole) {
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
    }

    @Benchmark
    public void SR5_listAllPartnerTypesGivenInputGoldenEntity(final Blackhole blackhole) {
        // SR5: GoldenEntity is known information: lookup by GoldenEntity id all Partner ids and Names
        blackhole.consume(
                g.V(getGoldenEntity()).
                // Go out to all partners.
                out("HAS_PARTNER").
                // Return the Partner type.
                elementMap("type").toList()
        );
    }
}

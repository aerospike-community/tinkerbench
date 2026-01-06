package com.aerospike;

import com.aerospike.idmanager.IdChainSampler;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;

class FmtArgInfoIdChainTest {

    private IdChainSampler idManager;

    //@BeforeEach
    FmtArgInfoIdChainTest() {
        final Integer[][] relationships = {
                {100, 200, 300, 400, 500, 600, 700, 800, 900}
        };
        final LogSource logSource = LogSource.getInstance();

        idManager = new IdChainSampler();
        idManager.addPath(relationships);
        assertTrue(idManager.CheckIdsExists(logSource), "There should be elements");
        assertEquals(9, idManager.getInitialDepth(), "Initial depth Check");
        assertEquals(9, idManager.getIdCount(), "Number of Ids Check");
        assertEquals(8, idManager.getNbrRelationships(),  "Number of Relationships Check");
        assertEquals(9, idManager.getDepth(),  "Depth Check");

        assertArrayEquals(relationships,
                            new Object[][] {  idManager.getIds() }, "Individual Id Check");

        //idManager.printStats(logSource);
    }

    @ParameterizedTest
    @MethodSource("provideGremlinStringsTesting")
    public void testGremlinStrFmts(String gString,
                                    String expectedGString,
                                    String expectedQuery,
                                    int nbrArgs,
                                    int depth,
                                    Object[] expectedIds) {
        final FmtArgInfo fmtInfo = new FmtArgInfo(gString, idManager);

        assertEquals(expectedGString, fmtInfo.gremlinString, String.format("Check Format Strings: '%s'", gString));
        assertEquals(fmtInfo.args.length, nbrArgs, String.format("Number of Args Check: '%s'", gString));
        assertEquals(fmtInfo.length(), nbrArgs, String.format("Check Length Check: '%s'", gString));
        assertEquals(fmtInfo.maxArgs(), depth,  String.format("Check Max Positions/Depth Check: '%s'", gString));
        assertArrayEquals(expectedIds, fmtInfo.getIds(), gString);

        String fmtStmt = fmtInfo.determineGremlinString();
        assertEquals(expectedQuery, String.format(fmtStmt, fmtInfo.getIds()), gString);
    }

    private static Stream<Arguments> provideGremlinStringsTesting() {
        return Stream.of(
                Arguments.of("g.V(%s).out().count().toList()",
                                "g.V(%1$s).out().count().toList()",
                                "g.V(100).out().count().toList()",
                                1,
                                1,
                                new Object[]{100}),
                Arguments.of("g.V(%s,%s).limit(4)",
                                "g.V(%1$s,%1$s).limit(4)",
                                "g.V(100,100).limit(4)",
                                2,
                                1,
                                new Object[]{100}),
                Arguments.of("g.V(%1$s).out().count()",
                                "g.V(%1$s).out().count()",
                                "g.V(100).out().count()",
                                1,
                                1,
                                new Object[]{100}),
                Arguments.of("g.V(%3$s).out().count()",
                                "g.V(%3$s).out().count()",
                                "g.V(300).out().count()",
                                1,
                                3,
                                new Object[]{100, 200, 300}),
                Arguments.of("g.V(%2$s).V(%s).outE().inV().hasId(%2$s)",
                        "g.V(%2$s).V(%1$s).outE().inV().hasId(%2$s)",
                        "g.V(200).V(100).outE().inV().hasId(200)",
                        3,
                        2,
                        new Object[]{100, 200}),
                Arguments.of("g.V(%3$s).V(%s).outE().inV().hasId(%2$s)",
                        "g.V(%3$s).V(%1$s).outE().inV().hasId(%2$s)",
                        "g.V(300).V(100).outE().inV().hasId(200)",
                        3,
                        3,
                        new Object[]{100, 200, 300}),
                Arguments.of("g.V(%1$s).outE().inV().hasId(%2$s)",
                                "g.V(%1$s).outE().inV().hasId(%2$s)",
                                "g.V(100).outE().inV().hasId(200)",
                                2,
                                2,
                                new Object[]{100, 200}),
                Arguments.of("g.V(%8$s).outE().inV().hasId(%4$s)",
                                "g.V(%8$s).outE().inV().hasId(%4$s)",
                                "g.V(800).outE().inV().hasId(400)",
                                2,
                                8,
                                new Object[]{100, 200, 300, 400, 500, 600, 700, 800}),
                Arguments.of("g.V(%1$s).V(%3$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                                "g.V(%1$s).V(%3$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                                "g.V(100).V(300).V(200).outE().V(600).inV().hasId(400)",
                                5,
                                6,
                                new Object[]{100, 200, 300, 400, 500, 600})
        );
    }
}
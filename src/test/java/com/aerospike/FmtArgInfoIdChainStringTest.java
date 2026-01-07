package com.aerospike;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FmtArgInfoIdChainStringTest extends FmtArgInfoIdChainTest<String> {

    final static String[][] relationships = {
            {"100", "200", "300", "400", "500", "600", "700", "800", "900"}
    };

    public FmtArgInfoIdChainStringTest() {
        super(relationships);

        assertEquals(8, idManager.getInitialDepth(), "Initial depth Check");
        assertEquals(9, idManager.getIdCount(), "Number of Ids Check");
        assertEquals(8, idManager.getNbrRelationships(),  "Number of Relationships Check");
        assertEquals(8, idManager.getDepth(),  "Depth Check");

    }

    @Override
    public Stream<Arguments> provideGremlinStringsTesting() {
        return Stream.of(
                Arguments.of("g.V(%s).out().count().toList()",
                                "g.V(\"%1$s\").out().count().toList()",
                                "g.V(\"100\").out().count().toList()",
                                1,
                                1,
                                false,
                                new Object[]{"100"}),
                Arguments.of("g.V(%s,'%s').limit(4)",
                                "g.V(\"%1$s\",'%1$s').limit(4)",
                                "g.V(\"100\",'100').limit(4)",
                                2,
                                1,
                                false,
                                new Object[]{"100"}),
                Arguments.of("g.V('%1$s').out().count()",
                                "g.V('%1$s').out().count()",
                                "g.V('100').out().count()",
                                1,
                                1,
                                false,
                                new Object[]{"100"}),
                Arguments.of("g.V(%3$s).out().count()",
                                "g.V(\"%3$s\").out().count()",
                                "g.V(\"300\").out().count()",
                                1,
                                3,
                                false,
                                new Object[]{"100", "200", "300"}),
                Arguments.of("g.V(\"%2$s\").V(%s).outE().inV().hasId(%2$s)",
                        "g.V(\"%2$s\").V(\"%1$s\").outE().inV().hasId(\"%2$s\")",
                        "g.V(\"200\").V(\"100\").outE().inV().hasId(\"200\")",
                        3,
                        2,
                        false,
                        new Object[]{"100", "200"}),
                Arguments.of("g.V(%3$s).V(%s).outE().inV().hasId(%2$s)",
                        "g.V(\"%3$s\").V(\"%1$s\").outE().inV().hasId(\"%2$s\")",
                        "g.V(\"300\").V(\"100\").outE().inV().hasId(\"200\")",
                        3,
                        3,
                        false,
                        new Object[]{"100", "200", "300"}),
                Arguments.of("g.V(%1$s).V(%-9$s).V(%2$s).outE().V('%-4$s').inV().hasId('%-12$s')",
                        "g.V(\"%1$s\").V(\"%1$s\").V(\"%2$s\").outE().V('%6$s').inV().hasId('%1$s')",
                        "g.V(\"100\").V(\"100\").V(\"200\").outE().V('600').inV().hasId('100')",
                        5,
                        6,
                        true,
                        new Object[]{"100", "200", "300", "400", "500", "600", "700", "800", "900"})
        );
    }
}
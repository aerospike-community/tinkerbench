package com.aerospike;

import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.provider.Arguments;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class FmtArgInfoIdChainIntTest extends FmtArgInfoIdChainTest<Integer> {

    final static Integer[][] relationships = {
            {100, 200, 300, 400, 500, 600, 700, 800, 900}
    };

    public FmtArgInfoIdChainIntTest() {
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
                                "g.V(phTBVar1).out().count().toList()",
                                "g.V(%1$s).out().count().toList()",
                                "g.V(100).out().count().toList()",
                                1,
                                1,
                                false,
                                new Object[]{100}),
                Arguments.of("g.V(%s,%s).limit(4)",
                                "g.V(phTBVar1,phTBVar1).limit(4)",
                                "g.V(%1$s,%1$s).limit(4)",
                                "g.V(100,100).limit(4)",
                                2,
                                1,
                                false,
                                new Object[]{100}),
                Arguments.of("g.V(%1$s).out().count()",
                                "g.V(phTBVar1).out().count()",
                                "g.V(%1$s).out().count()",
                                "g.V(100).out().count()",
                                1,
                                1,
                                false,
                                new Object[]{100}),
                Arguments.of("g.V(%3$s).out().count()",
                                "g.V(phTBVar3).out().count()",
                                "g.V(%3$s).out().count()",
                                "g.V(300).out().count()",
                                1,
                                3,
                                false,
                                new Object[]{100, 200, 300}),
                Arguments.of("g.V(%2$s).V(%s).outE().inV().hasId(%2$s)",
                        "g.V(phTBVar2).V(phTBVar1).outE().inV().hasId(phTBVar2)",
                        "g.V(%2$s).V(%1$s).outE().inV().hasId(%2$s)",
                        "g.V(200).V(100).outE().inV().hasId(200)",
                        3,
                        2,
                        false,
                        new Object[]{100, 200}),
                Arguments.of("g.V(%3$s).V(%s).outE().inV().hasId(%2$s)",
                        "g.V(phTBVar3).V(phTBVar1).outE().inV().hasId(phTBVar2)",
                        "g.V(%3$s).V(%1$s).outE().inV().hasId(%2$s)",
                        "g.V(300).V(100).outE().inV().hasId(200)",
                        3,
                        3,
                        false,
                        new Object[]{100, 200, 300}),
                Arguments.of("g.V(%1$s).outE().inV().hasId(%2$s)",
                                "g.V(phTBVar1).outE().inV().hasId(phTBVar2)",
                                "g.V(%1$s).outE().inV().hasId(%2$s)",
                                "g.V(100).outE().inV().hasId(200)",
                                2,
                                2,
                                false,
                                new Object[]{100, 200}),
                Arguments.of("g.V(%8$s).outE().inV().hasId(%4$s)",
                                "g.V(phTBVar8).outE().inV().hasId(phTBVar4)",
                                "g.V(%8$s).outE().inV().hasId(%4$s)",
                                "g.V(800).outE().inV().hasId(400)",
                                2,
                                8,
                                false,
                                new Object[]{100, 200, 300, 400, 500, 600, 700, 800}),
                Arguments.of("g.V(%1$s).V(%3$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                        "g.V(phTBVar1).V(phTBVar3).V(phTBVar2).outE().V(phTBVar6).inV().hasId(phTBVar4)",
                        "g.V(%1$s).V(%3$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                                "g.V(100).V(300).V(200).outE().V(600).inV().hasId(400)",
                                5,
                                6,
                                false,
                                new Object[]{100, 200, 300, 400, 500, 600}),
                Arguments.of("g.V(%1$s).V(%11$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                        "g.V(phTBVar1).V(phTBVar11).V(phTBVar2).outE().V(phTBVar6).inV().hasId(phTBVar4)",
                        "g.V(%1$s).V(%11$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                        "g.V(100).V(null).V(200).outE().V(600).inV().hasId(400)",
                        5,
                        11,
                        false,
                        new Object[]{100, 200, 300, 400, 500, 600, 700, 800, 900, null, null}),
                Arguments.of("g.V(%1$s).V(%-7$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                        "g.V(phTBVar1).V(phTBVar3).V(phTBVar2).outE().V(phTBVar6).inV().hasId(phTBVar4)",
                        "g.V(%1$s).V(%3$s).V(%2$s).outE().V(%6$s).inV().hasId(%4$s)",
                        "g.V(100).V(300).V(200).outE().V(600).inV().hasId(400)",
                        5,
                        6,
                        true,
                        new Object[]{100, 200, 300, 400, 500, 600, 700, 800, 900}),
                Arguments.of("g.V(%1$s).V(%-7$s).V(%2$s).outE().V(%6$s).inV().hasId(%-7$s)",
                        "g.V(phTBVar1).V(phTBVar3).V(phTBVar2).outE().V(phTBVar6).inV().hasId(phTBVar3)",
                        "g.V(%1$s).V(%3$s).V(%2$s).outE().V(%6$s).inV().hasId(%3$s)",
                        "g.V(100).V(300).V(200).outE().V(600).inV().hasId(300)",
                        5,
                        6,
                        true,
                        new Object[]{100, 200, 300, 400, 500, 600, 700, 800, 900}),
                Arguments.of("g.V(%1$s).V(%-9$s).V(%2$s).outE().V(%-4$s).inV().hasId(%-12$s)",
                        "g.V(phTBVar1).V(phTBVar1).V(phTBVar2).outE().V(phTBVar6).inV().hasId(phTBVar1)",
                        "g.V(%1$s).V(%1$s).V(%2$s).outE().V(%6$s).inV().hasId(%1$s)",
                        "g.V(100).V(100).V(200).outE().V(600).inV().hasId(100)",
                        5,
                        6,
                        true,
                        new Object[]{100, 200, 300, 400, 500, 600, 700, 800, 900})
        );
    }
}
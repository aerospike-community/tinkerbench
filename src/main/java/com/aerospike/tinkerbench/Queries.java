package com.aerospike.tinkerbench;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.UUID;

public class Queries {
    public static final String TEST_VERTEX = "testVertex";
    public static final String KNOWS = "knows";
    public static void coalesce(GraphTraversalSource g) {
        final String vertexId1 = UUID.randomUUID().toString();
        final String vertexId2 = UUID.randomUUID().toString();
        g
                .V(vertexId1).fold()
                .coalesce(
                        __.unfold(),
                        __.addV(TEST_VERTEX).property(T.id, vertexId1)
                )
                .V(vertexId2).fold()
                .coalesce(
                        __.unfold(),
                        __.addV(TEST_VERTEX).property(T.id, vertexId2)
                )
                .outE(KNOWS)
                .where(__.inV().hasId(vertexId1))
                .fold()
                .coalesce(
                        __.unfold(),
                        __.addE(KNOWS).from(__.V(vertexId2)).to(__.V(vertexId1))
                ).iterate();
    }

    public static void insertVVE(GraphTraversalSource g) {
        final String vertexId1 = UUID.randomUUID().toString();
        final String vertexId2 = UUID.randomUUID().toString();
        g.addV(TEST_VERTEX).property(T.id, vertexId1).addE(KNOWS).to(__.addV(TEST_VERTEX).property(T.id, vertexId2)).iterate();
    }
}

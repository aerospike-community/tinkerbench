package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface IdManager {
    void init(final GraphTraversalSource g);
    void init(final GraphTraversalSource g, final int sampleSize, String label);

    boolean isInitialized();
    Object getId();
}

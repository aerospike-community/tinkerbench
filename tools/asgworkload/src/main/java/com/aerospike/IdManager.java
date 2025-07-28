package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface IdManager {
    void init(GraphTraversalSource g);
    Object getId();
}

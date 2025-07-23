package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface AGSGraphTraversal {

    GraphTraversalSource G();
    Cluster getCluster();
}

package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;

public interface AGSGraphTraversal {

    GraphTraversalSource G();
    List<GraphTraversalSource> Gs();
    Cluster getCluster();
}

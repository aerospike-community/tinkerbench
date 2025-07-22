package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.Closeable;

public interface AGSGraphTraversal extends Closeable {

    public GraphTraversalSource G();
    public Cluster getCluster();
}

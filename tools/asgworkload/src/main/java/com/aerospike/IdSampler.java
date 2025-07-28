package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.List;
import java.util.Random;

public class IdSampler implements  IdManager {
    // TODO: Make this configurable.
    private static final int ID_SAMPLE_SIZE = 500_000;
    private List<Object> sampledIds = null;
    final Random random = new Random();

    public IdSampler() {
        // Constructor can be used for initialization if needed
    }

    @Override
    public void init(GraphTraversalSource g) {
        sampledIds = g.V().limit(ID_SAMPLE_SIZE).id().toList();
    }

    @Override
    public Object getId() {
        return sampledIds.get(random.nextInt(sampledIds.size()));
    }
}

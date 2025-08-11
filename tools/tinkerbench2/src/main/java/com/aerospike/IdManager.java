package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface IdManager {
    void init(final GraphTraversalSource g,
                final OpenTelemetry openTelemetry,
                final LogSource logger,
                final int sampleSize,
                final String label);

    boolean isInitialized();
    Object getId();
}

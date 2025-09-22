package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.File;

public interface IdManager {
    void init(final GraphTraversalSource g,
                final OpenTelemetry openTelemetry,
                final LogSource logger,
                final int sampleSize,
                final String label);

    boolean isInitialized();
    Object getId();

    long importFile(final String file,
                       final OpenTelemetry openTelemetry,
                       final LogSource logger,
                       final int sampleSize,
                       final String label);
    void exportFile(final String file);
}

package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.File;

public interface IdManager {

    boolean CheckIdsExists(final LogSource logger);

    void init(final GraphTraversalSource g,
                final OpenTelemetry openTelemetry,
                final LogSource logger,
                final int sampleSize,
                final String[] labels);

    boolean isInitialized();
    Object getId();

    long importFile(final String file,
                       final OpenTelemetry openTelemetry,
                       final LogSource logger,
                       final int sampleSize,
                       final String[] labels);

    void exportFile(final String filePath,
                    final LogSource logger);
}

package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.management.Query;

public interface IdManagerQuery extends IdManager {

    default void init(final GraphTraversalSource g,
                      final OpenTelemetry openTelemetry,
                      final LogSource logger,
                      final int sampleSize,
                      final String[] labels) {
        throw new UnsupportedOperationException("This method is not supported with this interface. Use the supported Init method instead.");
    }

    /*
     *   Obtains Id from the Graph DB
     */
    void init(final AGSGraphTraversal ags,
              final OpenTelemetry openTelemetry,
              final LogSource logger,
              final String gremlinString);
}

package com.aerospike;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class AirRoutesQuery extends QueryWorkloadProvider {

    public AirRoutesQuery(final WorkloadProvider provider,
                          final AGSGraphTraversal ags) {
        super(provider, ags);
    }

    /**
     * @return thw Query name
     */
    @Override
    public String Name() {
        return "AirRoutes";
    }

    /**
     * @return The query description
     */
    @Override
    public String getDescription() {
        return "Air Routes Graph Traversal";
    }

    /**
     * Performs any required pre-process required for the query.
     * @return true to execute workload or false to abort execution
     */
    @Override
    public boolean preProcess() {
        return true;
    }

    /**
     * Performs any post-processing for the query
     */
    @Override
    public void postProcess() {

    }

    /**
     * @return true to indicate that the workload was successful and should be recorded.
     *  False to indicate that the workload should not be recorded. This also occurs for any exceptions
     * @throws Exception
     */
    @Override
    public Boolean call() throws Exception {
        G().V(3).out().limit(5).path().by(values("code","city").fold()).toList();
        return true;
    }
}

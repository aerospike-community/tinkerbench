package com.aerospike;

import org.apache.tinkerpop.gremlin.structure.T;
import org.javatuples.Pair;

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
     *  False to indicate that the workload should not be recorded.
     *  If an exception occurs the exception is only recorded. If an InterruptedException occurs, this is treated like a false return.
     */
    @Override
    public Pair<Boolean,Object> call() throws Exception {
        G().V(3).out().limit(5).path().by(values("code","city").fold()).toList();
        return new Pair<>(true,null);
    }

    /*
   Called before the actual workload is executed.
   This is called within the scheduler and is NOT part of the workload measurement.
    */
    @Override
    public void preCall() {}

    /*
    Called after the actual workload is executed passing the value type T from the workload.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    @Override
    public void postCall(Object ignored0, Boolean ignored1, Throwable ignored2) {}
}

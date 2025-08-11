package com.aerospike.predefined;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManager;
import com.aerospike.QueryWorkloadProvider;
import com.aerospike.WorkloadProvider;
import org.javatuples.Pair;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class AirRoutesQuery1 extends QueryWorkloadProvider {

    public AirRoutesQuery1(final WorkloadProvider provider,
                           final AGSGraphTraversal ags,
                           final IdManager idManager) {
        super(provider, ags, idManager);
    }

    /**
     * @return thw Query name
     */
    @Override
    public String Name() {
        return "AirRoutesQuery1";
    }

    /**
     * @return The query description
     */
    @Override
    public String getDescription() {
        return "Air Routes Graph Traversal Query";
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
        G().V( getVId() ).out().limit(5).path().by(values("code","city").fold()).toList();
        return new Pair<>(true,null);
    }

    /*
   Called before the actual workload is executed.
   This is called within the scheduler and is NOT part of the workload measurement.
    */
    @Override
    public void preCall() {}

    /*
    Called after the actual workload is executed passing the value from the workload.
        The success param is true if the workload was recorded and exception will be not-null if an exciton occurred during workload execution.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    @Override
    public void postCall(Object value, Boolean success, Throwable error) {}
}

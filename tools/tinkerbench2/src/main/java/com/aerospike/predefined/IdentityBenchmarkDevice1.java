package com.aerospike.predefined;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManager;
import com.aerospike.QueryWorkloadProvider;
import com.aerospike.WorkloadProvider;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.List;

public class IdentityBenchmarkDevice1 extends QueryWorkloadProvider {

    public IdentityBenchmarkDevice1(final WorkloadProvider provider,
                                      final AGSGraphTraversal ags,
                                      final IdManager idManager) {
        super(provider, ags, idManager);
    }

    /**
     * @return thw Query name
     */
    @Override
    public String Name() {
        return "IdentityBenchmarkDevice1";
    }

    /**
     * @return The query description
     */
    @Override
    public String getDescription() {
        return "Identity Benchmark Query:\n\tG().V(<DeviceId>).in('HAS_DEVICE', 'PROVIDED_DEVICE').out('HAS_DEVICE', 'PROVIDED_DEVICE').toList();";
    }

    /*
    Returns a label used to obtain the ids in the sampling.
        If a label is provided in the CLI, that one would override this value!
     */
    @Override
    public final String getSampleLabelId() { return "Device"; }

    /**
     * @return true to indicate that the workload was successful and should be recorded.
     *  False to indicate that the workload should not be recorded.
     *  If an exception occurs the exception is only recorded. If an InterruptedException occurs, this is treated like a false return.
     *  The second item in the return is null since there is no post-call processing required.
     */
    @Override
    public Pair<Boolean,Object> call() throws Exception {
        final List<Vertex> result =  G().V( getVId() )
                                        .in("HAS_DEVICE", "PROVIDED_DEVICE")
                                        .out("HAS_DEVICE", "PROVIDED_DEVICE")
                                        .toList();

        //The below code is not required since the result can be ignored.
        if(isPrintResult())
            result.forEach(this::PrintResult);

        return new Pair<>(true,null);
    }
}

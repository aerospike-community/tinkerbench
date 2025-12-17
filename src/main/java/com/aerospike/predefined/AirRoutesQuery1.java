package com.aerospike.predefined;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManager;
import com.aerospike.QueryWorkloadProvider;
import com.aerospike.WorkloadProvider;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.javatuples.Pair;

import java.util.List;

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
        return "Air Routes Graph Traversal Query:\n\tG().V( getVId() ).out().limit(5).path().by(values(\"code\",\"city\").fold()).toList();";
    }

    /*
        @return Return zero for starting id (top-level) only.
     */
    @Override
    public int getVDepth() { return 0; }

    /**
     * @return true to indicate that the workload was successful and should be recorded.
     *  False to indicate that the workload should not be recorded.
     *  If an exception occurs the exception is only recorded. If an InterruptedException occurs, this is treated like a false return.
     *  The second item in the return is null since there is no post-call processing required.
     */
    @Override
    public Pair<Boolean,Object> call() throws Exception {
        final List<Path> result =  G().V( getVId() )
                                            .out()
                                            .limit(5)
                                            .path()
                                            .by(values("code","city")
                                                    .fold())
                                            .toList();

        //The below code is not required since the result can be ignored.
        if(isPrintResult())
            result.forEach(this::PrintResult);

        return new Pair<>(true,null);
    }
}

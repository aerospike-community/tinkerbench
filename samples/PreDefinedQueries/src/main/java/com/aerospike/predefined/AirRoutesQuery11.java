package com.aerospike.predefined;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManager;
import com.aerospike.WorkloadProvider;

public class AirRoutesQuery11 extends AirRoutesQuery1
{
    public AirRoutesQuery11(final WorkloadProvider provider,
                           final AGSGraphTraversal ags,
                           final IdManager idManager) {
        super(provider, ags, idManager);
    }

    @Override
    public String Name()
    {
        return "AirRoutesQuery11";
    }
}

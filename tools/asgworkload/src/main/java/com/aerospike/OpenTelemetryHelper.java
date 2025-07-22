package com.aerospike;

public final class OpenTelemetryHelper {

    public static OpenTelemetry Create(AGSWorkloadArgs args,
                                       StringBuilder otherInfo) {

        if(args.promPort > 0) {
            return new OpenTelemetryExporter(args,
                                                otherInfo);
        }

        return new OpenTelemetryDummy(args,
                                       otherInfo);
    }

}

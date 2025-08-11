package com.aerospike;

public final class OpenTelemetryHelper {

    public static OpenTelemetry Create(TinkerBench2Args args,
                                       StringBuilder otherInfo) {

        if(args.promEnabled) {
            return new OpenTelemetryExporter(args,
                                                otherInfo);
        }

        return new OpenTelemetryDummy(args,
                                       otherInfo);
    }

}

package com.aerospike;

public final class OpenTelemetryHelper {

    public static OpenTelemetry Create(int endPointPort,
                                       AGSWorkloadArgs args,
                                       int closeWaitMS,
                                       StringBuilder otherInfo) {

        if(endPointPort > 0) {
            return new OpenTelemetryExporter(endPointPort,
                                                args,
                                                closeWaitMS,
                                                otherInfo);
        }

        return new OpenTelemetryDummy(endPointPort,
                                        args,
                                        closeWaitMS,
                                        otherInfo);
    }

}

package com.aerospike;

public class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(int endPointPort,
                                AGSWorkloadArgs args,
                                int closeWaitMS,
                                StringBuilder otherInfo) {
    }

    public OpenTelemetryDummy() {
    }

    @Override
    public boolean getClosed() { return false; }

    @SuppressWarnings("unused")
    @Override
    public void incrTransCounter() {
    }
    @Override
    public void incrTransCounter(String type) {
    }

    @SuppressWarnings("unused")
    @Override
    public void addException(Exception exception) {
    }

    @SuppressWarnings("unused")
    @Override
    public void addException(String exceptionType, String exception_subtype, String message) {
    }

    @SuppressWarnings("unused")
    @Override
    public void recordElapsedTime(String type, long elapsedNanos) {
    }

    @SuppressWarnings("unused")
    @Override
    public void recordElapsedTime(long elapsedNanos) {
    }

    @Override
    public void close() throws Exception {
    }

    @SuppressWarnings("unused")
    @Override
    public void setWorkloadName(String workLoadType, String workloadName) {
    }

    @SuppressWarnings("unused")
    @Override
    public void setConnectionState(String connectionState){
    }

    @Override
    public String printConfiguration() {
        return "Open Telemetry Disabled";
    }

    @Override
    public String toString() {
        return "OpenTelemetryDummy{}";
    }
}

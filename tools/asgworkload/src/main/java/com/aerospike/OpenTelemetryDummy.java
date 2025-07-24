package com.aerospike;

import java.time.Duration;

public final class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(AGSWorkloadArgs args,
                                StringBuilder otherInfo) {
    }

    public OpenTelemetryDummy() {
    }

    @Override
    public boolean getClosed() { return false; }

    @Override
    public void Reset(AGSWorkloadArgs args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      boolean warmup,
                      StringBuilder otherInfo) { }

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
    public void addException(String exceptionType, String message) {
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
    public void setWorkloadName(String workLoadType, String workloadName, boolean warmup) {
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

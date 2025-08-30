package com.aerospike;

import java.time.Duration;

public final class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(TinkerBench2Args args,
                              StringBuilder otherInfo) {
    }

    public OpenTelemetryDummy() {
    }

    @Override
    public boolean getClosed() { return false; }

    @Override
    public boolean isEnabled() { return false; }

    @Override
    public void Reset(TinkerBench2Args args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      long pendingActions,
                      boolean warmup,
                      StringBuilder otherInfo) { }
    @Override
    public void setIdMgrGauge(final String mgrClass,
                              final String label,
                              final int requestedCnt,
                              final int actualCnt,
                              final long runtime) {
    }

    @SuppressWarnings("unused")
    @Override
    public void incrPendingTransCounter() {
    }

    @Override
    public void decrPendingTransCounter() {}

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
    public void recordElapsedTime(long elapsedNanos) {
    }

    @Override
    public void close() throws Exception {
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

package com.aerospike;

import java.time.Duration;

public final class OpenTelemetryDummy  implements OpenTelemetry {

    @SuppressWarnings("unused")
    public OpenTelemetryDummy(TinkerBenchArgs args,
                              StringBuilder otherInfo) {
    }

    public OpenTelemetryDummy() {
    }

    @Override
    public boolean getClosed() { return false; }

    @Override
    public boolean isEnabled() { return false; }

    @Override
    public void Reset(TinkerBenchArgs args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      long pendingActions,
                      boolean warmup,
                      boolean warmupRan,
                      StringBuilder otherInfo) { }
    @Override
    public void setIdMgrGauge(final String mgrClass,
                              final String[] labels,
                              final String gremlinString,
                              final int distinctNodeCnt,
                              final int rootNodesCnt,
                              final int requestedDepth,
                              final int possibleDepth,
                              final int relationships,
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
    public void recordElapsedTime(long elapsedNanos, double currentQPS) {
    }

    @Override
    public void close() throws Exception {
    }

    @SuppressWarnings("unused")
    @Override
    public void setConnectionState(String connectionState, int currentCPS){
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

package com.aerospike;

import java.time.Duration;

public interface OpenTelemetry extends AutoCloseable {

    boolean getClosed();
    void Reset(AGSWorkloadArgs args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      long pendingActions,
                      boolean warmup,
                      StringBuilder otherInfo);

    void addException(Exception exception);
    void addException(String exceptionType, String message);

    void recordElapsedTime(long elapsedNanos);

    void incrPendingTransCounter();
    void decrPendingTransCounter();

    void setWorkloadName(String workLoadType, String workloadName, boolean warmup);
    void setConnectionState(String connectionState);

    String printConfiguration();
}

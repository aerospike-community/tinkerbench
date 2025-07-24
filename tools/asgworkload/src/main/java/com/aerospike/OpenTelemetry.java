package com.aerospike;

import java.time.Duration;

public interface OpenTelemetry extends AutoCloseable {

    boolean getClosed();
    void Reset(AGSWorkloadArgs args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      boolean warmup,
                      StringBuilder otherInfo);

    void addException(Exception exception);
    void addException(String exceptionType, String message);

    void recordElapsedTime(String type, long elapsedNanos);
    void recordElapsedTime(long elapsedNanos);

    void incrTransCounter();
    void incrTransCounter(String type);

    void setWorkloadName(String workLoadType, String workloadName, boolean warmup);
    void setConnectionState(String connectionState);

    String printConfiguration();
}

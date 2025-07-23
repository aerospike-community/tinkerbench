package com.aerospike;

public interface OpenTelemetry extends AutoCloseable {

    public boolean getClosed();
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

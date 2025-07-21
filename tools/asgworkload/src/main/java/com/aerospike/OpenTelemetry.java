package com.aerospike;

public interface OpenTelemetry extends AutoCloseable {

    public boolean getClosed();
    void addException(Exception exception);
    void addException(String exceptionType, String exception_subtype, String message);

    void recordElapsedTime(String type, long elapsedNanos);
    void recordElapsedTime(long elapsedNanos);

    void incrTransCounter();
    void incrTransCounter(String type);

    void setWorkloadName(String workloadName);
    void setDBConnectionState(String dbConnectionState);

    String printConfiguration();
}

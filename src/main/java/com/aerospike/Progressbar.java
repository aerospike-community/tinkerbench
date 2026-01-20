package com.aerospike;

import java.util.concurrent.atomic.AtomicBoolean;

public class Progressbar implements AutoCloseable {

    private final WorkloadProvider workloadProvider;
    private final ProgressBarBuilder.ProgressBar underlyingProgressBar;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean isDebug;

    public Progressbar(WorkloadProvider workload) {
        this.workloadProvider = workload;
        this.isDebug = workload.isDebug();

        this.underlyingProgressBar = ProgressBarBuilder.Builder(workload.getCliArgs().backgroundMode)
                                        .setInitialMax(this.workloadProvider
                                                        .getTargetRunDuration()
                                                        .toSeconds())
                                        .setTaskName(this.workloadProvider.isWarmup()
                                                        ? "Warmup" : "Workload")
                                        .hideEta()
                                        .setUpdateIntervalMillis(1000)
                                        .setUnit(" Seconds", 1)
                                        .build();
    }

    public synchronized void start(String msg) {
        if(closed.get()) { return; }
        this.underlyingProgressBar.step();
        if(msg != null) {
            this.underlyingProgressBar.setExtraMessage(msg);
        }
        if(isDebug) {
            System.out.println();
        }
    }

    public void start() {
        start(null);
    }

    void setRateMessage(String msg) {
        String rateMsg = String.format("OPS: %,d Pending: %,d Errors: %,d",
                Math.round(workloadProvider.getCallsPerSecond()),
                workloadProvider.getPendingCount(),
                workloadProvider.getErrorCount());

        if (msg != null) {
            rateMsg += "; " + msg;
        }
        if (isDebug) {
            rateMsg += String.format("%n");
        }
        this.underlyingProgressBar.setExtraMessage(rateMsg);
    }

    long determineDelta() {
        if(this.underlyingProgressBar
                .getTotalElapsed().toSeconds() >= this.underlyingProgressBar.getMax()
            || this.underlyingProgressBar.getCurrent() >= this.underlyingProgressBar.getMax()) {
            return -1;
        } else {
            return this.underlyingProgressBar
                                    .getElapsedAfterStart()
                                    .toSeconds()
                                - this.underlyingProgressBar.getCurrent();
        }
    }

    public synchronized void step(String msg) {
        if(closed.get()) { return; }

        long delta = determineDelta();

        if(delta > 0) {
            this.underlyingProgressBar.stepBy(delta);
        }
        setRateMessage(msg);
    }

    public void step() { this.step(null); }

    public synchronized void stop(String msg) {
        if(closed.get()) { return; }

        closed.set(true);
        if(!workloadProvider.isAborted()){
            final long delta = this.underlyingProgressBar.getMax() - this.underlyingProgressBar.getCurrent();
            if(delta > 0) {
                this.underlyingProgressBar.stepBy(delta);
                this.underlyingProgressBar.refresh();
            }
        }
        if(msg != null) {
            this.underlyingProgressBar.setExtraMessage(msg);
        }
        this.underlyingProgressBar.refresh();
        try {
            Thread.sleep(100);
        } catch (InterruptedException ignored) {}

        this.underlyingProgressBar.close();

        System.out.println();
    }

    public void stop() { this.stop(null); }

    @Override
    public synchronized void close() {
        if(!closed.get()) {
            closed.set(true);
            underlyingProgressBar.close();
        }
    }
}

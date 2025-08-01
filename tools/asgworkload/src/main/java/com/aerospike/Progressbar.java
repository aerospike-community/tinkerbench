package com.aerospike;

import me.tongfei.progressbar.ConsoleProgressBarConsumer;
import me.tongfei.progressbar.ProgressBarBuilder;
import me.tongfei.progressbar.ProgressBarConsumer;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class Progressbar implements AutoCloseable {

    private final WorkloadProvider workloadProvider;
    private final me.tongfei.progressbar.ProgressBar underlyingProgressBar;
    private final ProgressBarConsumer consoleConsumer;
    private final AtomicLong lastPrint = new AtomicLong(0);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean isDebug;

    public Progressbar(WorkloadProvider workload) {
        this.workloadProvider = workload;
        this.isDebug = workload.isDebug();

        consoleConsumer = new ConsoleProgressBarConsumer(System.out);
        this.underlyingProgressBar = new ProgressBarBuilder()
                .setInitialMax(this.workloadProvider
                                    .getTargetRunDuration()
                                    .toSeconds())
                .setTaskName(this.workloadProvider.isWarmup()
                                ? "Warmup" : "Workload")
                .hideEta()
                .setConsumer(consoleConsumer)
                .setUpdateIntervalMillis(1000)
                .setUnit(" Seconds", 1)
                .build();
    }

    public void start(String msg) {
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

    long determineDelta() {
        if(this.underlyingProgressBar
                .getTotalElapsed().toSeconds() >= this.underlyingProgressBar.getMax()
            || this.underlyingProgressBar.getCurrent() >= this.underlyingProgressBar.getMax()) {
            return -1;
        } else {
            long delta = this.underlyingProgressBar
                            .getElapsedAfterStart()
                            .toSeconds()
                            - this.lastPrint.get();
            if(delta > this.underlyingProgressBar.getMax()) {
                return -1;
            } else {
                return delta;
            }
        }
    }

    public void step(String msg) {
        if(closed.get()) { return; }

        long delta = determineDelta();

        if(delta > 0) {
            this.underlyingProgressBar.stepBy(delta);
            this.lastPrint.set(this.underlyingProgressBar
                                    .getElapsedAfterStart()
                                    .toSeconds());
        }
        String rateMsg = String.format("OPS: %,d Pending: %,d Errors: %,d",
                            Math.round(workloadProvider.getCallsPerSecond()),
                            workloadProvider.getPendingCount(),
                            workloadProvider.getErrorCount());

        if(msg != null) {
            rateMsg += "; " + msg;
        }
        if(isDebug) {
            rateMsg += String.format("%n");
        }
        this.underlyingProgressBar.setExtraMessage(rateMsg);
    }

    public void step() { this.step(null); }

    public void stop(String msg) {
        if(closed.get()) { return; }

        closed.set(true);
        if(!workloadProvider.isAborted()){
            if(this.underlyingProgressBar.getCurrent() < this.underlyingProgressBar.getMax()) {
                this.underlyingProgressBar.step();
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
        this.consoleConsumer.close();

        System.out.println();
    }

    public void stop() { this.stop(null); }

    @Override
    public void close() {
        if(!closed.get()) {
            underlyingProgressBar.close();
            consoleConsumer.close();
        }
    }
}

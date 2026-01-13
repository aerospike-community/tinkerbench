package com.aerospike;

import me.tongfei.progressbar.*;
import me.tongfei.progressbar.wrapped.*;

import java.io.*;
import java.text.DecimalFormat;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

public final class ProgressBarBuilder implements AutoCloseable {

    public static final class ProgressBar implements AutoCloseable {
        private final ProgressBarBuilder progressBuilder;
        private final me.tongfei.progressbar.ProgressBar  progress;
        private final AtomicBoolean closed = new AtomicBoolean(false);

        public ProgressBar(ProgressBarBuilder progressBuilder) {
            this.progressBuilder = progressBuilder;
            this.progress = progressBuilder.getProgressbarBuilder().build();
        }

        public ProgressBar stepBy(long n) {
            this.progress.stepBy(n);
            return this;
        }

        public ProgressBar stepTo(long n) {
           this.progress.stepTo(n);
            return this;
        }

        public ProgressBar step() {
            this.progress.stepBy(1L);
            return this;
        }

        public ProgressBar maxHint(long n) {
            this.progress.maxHint(n);
            return this;
        }

        public ProgressBar pause() {
            this.progress.pause();
            return this;
        }

        public ProgressBar resume() {
            this.progress.resume();
            return this;
        }

        public ProgressBar reset() {
            this.progress.reset();
            return this;
        }

        public void close() {
            if(!closed.get()) {
                closed.set(true);
                this.progress.close();
            }
            this.progressBuilder.close();
        }

        public ProgressBar setExtraMessage(String msg) {
            this.progress.setExtraMessage(msg);
            return this;
        }

        public long getCurrent() {
            return this.progress.getCurrent();
        }

        public long getMax() {
            return this.progress.getMax();
        }

        public long getStart() {
            return this.progress.getStart();
        }

        public double getNormalizedProgress() {
            return this.progress.getNormalizedProgress();
        }

        public Instant getStartInstant() {
            return this.progress.getStartInstant();
        }

        public Duration getElapsedBeforeStart() {
            return this.progress.getElapsedBeforeStart();
        }

        public Duration getElapsedAfterStart() {
            return this.progress.getElapsedAfterStart();
        }

        public Duration getTotalElapsed() {
            return this.progress.getTotalElapsed();
        }

        public String getTaskName() {
            return this.progress.getTaskName();
        }

        public String getExtraMessage() {
            return this.progress.getExtraMessage();
        }

        public boolean isIndefinite() {
            return this.progress.isIndefinite();
        }

        public void refresh() {
            this.progress.refresh();
        }

        public me.tongfei.progressbar.ProgressBar getProgressBar() {
            return this.progress;
        }
        public boolean isClosed() { return this.closed.get(); }
    }

    private final me.tongfei.progressbar.ProgressBarBuilder progressbarBuilder;
    private final ProgressBarConsumer consoleConsumer;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final boolean backGround;

    public ProgressBarBuilder() {
        this(TinkerBenchArgs.inBackgroundMode);
    }

    public ProgressBarBuilder(boolean backGround) {

        this.progressbarBuilder = new me.tongfei.progressbar.ProgressBarBuilder();
        this.backGround = backGround;
        if(this.backGround) {
            consoleConsumer = new DelegatingProgressBarConsumer( (s) -> System.out.print(".") );
        } else {
            consoleConsumer = new ConsoleProgressBarConsumer(System.out);
        }

        this.setConsumer(consoleConsumer);
    }

    public static ProgressBarBuilder Builder(boolean backGround) {
        return new ProgressBarBuilder(backGround);
    }

    public static ProgressBarBuilder Builder() {
        return ProgressBarBuilder.Builder(TinkerBenchArgs.inBackgroundMode);
    }

    public boolean isClosed() { return this.closed.get(); }
    public boolean inBackGroundMode() { return this.backGround; }
    public me.tongfei.progressbar.ProgressBarBuilder getProgressbarBuilder() { return this.progressbarBuilder; }

    public ProgressBarBuilder setTaskName(String task) {
        this.progressbarBuilder.setTaskName(task);
        return this;
    }

    public ProgressBarBuilder setInitialMax(long initialMax) {
        this.progressbarBuilder.setInitialMax(initialMax);
        return this;
    }

    public ProgressBarBuilder setStyle(ProgressBarStyle style) {
        this.progressbarBuilder.setStyle(style);
        return this;
    }

    public ProgressBarBuilder setUpdateIntervalMillis(int updateIntervalMillis) {
        this.progressbarBuilder.setUpdateIntervalMillis(updateIntervalMillis);
        return this;
    }

    public ProgressBarBuilder continuousUpdate() {
        this.progressbarBuilder.continuousUpdate();
        return this;
    }

    public ProgressBarBuilder setConsumer(ProgressBarConsumer consumer) {
        this.progressbarBuilder.setConsumer(consumer);
        return this;
    }

    public ProgressBarBuilder clearDisplayOnFinish() {
        this.progressbarBuilder.clearDisplayOnFinish();
        return this;
    }

    public ProgressBarBuilder setUnit(String unitName, long unitSize) {
        this.progressbarBuilder.setUnit(unitName, unitSize);
        return this;
    }

    public ProgressBarBuilder setMaxRenderedLength(int maxRenderedLength) {
        this.progressbarBuilder.setMaxRenderedLength(maxRenderedLength);
        return this;
    }

    public ProgressBarBuilder setRenderer(ProgressBarRenderer renderer) {
        this.progressbarBuilder.setRenderer(renderer);
        return this;
    }

    public ProgressBarBuilder showSpeed() {
        return this.showSpeed(new DecimalFormat("#.0"));
    }

    public ProgressBarBuilder showSpeed(DecimalFormat speedFormat) {
        this.progressbarBuilder.showSpeed(speedFormat);
        return this;
    }

    public ProgressBarBuilder hideEta() {
        this.progressbarBuilder.hideEta();
        return this;
    }

    public ProgressBarBuilder setEtaFunction(Function<ProgressState, Optional<Duration>> eta) {
        this.progressbarBuilder.setEtaFunction(eta);
        return this;
    }

    public ProgressBarBuilder setSpeedUnit(ChronoUnit speedUnit) {
        this.progressbarBuilder.setSpeedUnit(speedUnit);
        return this;
    }

    public ProgressBarBuilder startsFrom(long processed, Duration elapsed) {
        this.progressbarBuilder.startsFrom(processed, elapsed);
        return this;
    }

    public ProgressBar build() {
        return new ProgressBar(this);
    }

    @Override
    public synchronized void close() {
        if(!closed.get()) {
            closed.set(true);
            consoleConsumer.close();
        }
    }
}

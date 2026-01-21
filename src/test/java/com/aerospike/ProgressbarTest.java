package com.aerospike;

import java.lang.reflect.Field;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unused")
class ProgressbarTest {

    /**
     * Minimal concrete args for tests to satisfy WorkloadProvider.getCliArgs().
     */
    static class TestArgs extends TinkerBenchArgs {
        @Override
        public Integer call() {
            return 0;
        }
    }

    /**
     * Simple WorkloadProvider stub with configurable values.
     */
    static class StubWorkload implements WorkloadProvider {
        private final boolean warmup;
        private final boolean debug;
        private final boolean aborted;
        private final Duration targetDuration;
        private final TinkerBenchArgs args;
        private double cps = 0;
        private long pending = 0;
        private long errors = 0;

        StubWorkload(boolean warmup, boolean debug, boolean aborted, Duration targetDuration, boolean backgroundMode) {
            this.warmup = warmup;
            this.debug = debug;
            this.aborted = aborted;
            this.targetDuration = targetDuration;
            this.args = new TestArgs();
            this.args.backgroundMode = backgroundMode;
        }

        void setMetrics(double cps, long pending, long errors) {
            this.cps = cps;
            this.pending = pending;
            this.errors = errors;
        }

        @Override public boolean isWarmup() { return warmup; }
        @Override public boolean isAborted() { return aborted; }
        @Override public boolean isDebug() { return debug; }
        @Override public int getTargetCallsPerSecond() { return 0; }
        @Override public Duration getTargetRunDuration() { return targetDuration; }
        @Override public Duration getAccumDuration() { return Duration.ZERO; }
        @Override public long getPendingCount() { return pending; }
        @Override public long getAbortedCount() { return 0; }
        @Override public Duration getAccumSuccessDuration() { return Duration.ZERO; }
        @Override public long getSuccessCount() { return 0; }
        @Override public long getErrorCount() { return errors; }
        @Override public java.util.List<Exception> getErrors() { return java.util.Collections.emptyList(); }
        @Override public Duration getAccumErrorDuration() { return Duration.ZERO; }
        @Override public WorkloadStatus getStatus() { return null; }
        @Override public java.time.LocalDateTime getStartDateTime() { return null; }
        @Override public java.time.LocalDateTime getCompletionDateTime() { return null; }
        @Override public Duration getRemainingTime() { return Duration.ZERO; }
        @Override public double getCallsPerSecond() { return cps; }
        @Override public double getCPSDiffPct() { return 0; }
        @Override public double getErrorsPerSecond() { return 0; }
        @Override public OpenTelemetry getOpenTelemetry() { return null; }
        @Override public TinkerBenchArgs getCliArgs() { return args; }
        @Override public WorkloadProvider setQuery(QueryRunnable queryRunnable) { return this; }
        @Override public WorkloadProvider Start() { return this; }
        @Override public WorkloadProvider Shutdown() { return this; }
        @Override public boolean awaitTermination() { return true; }
        @Override public WorkloadProvider PrintSummary() { return this; }
        @Override public void SignalAbortWorkLoad() { }
        @Override public void SignalAbortWorkLoadPrintResults() { }
        @Override public long AddError(Exception e) { return 0; }
        @Override public void close() { }
    }

    private ProgressBarBuilder.ProgressBar extractUnderlying(Progressbar progressbar) {
        try {
            Field f = Progressbar.class.getDeclaredField("underlyingProgressBar");
            f.setAccessible(true);
            return (ProgressBarBuilder.ProgressBar) f.get(progressbar);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Nested
    @DisplayName("start() behavior")
    class StartTests {
        @Test
        void startStepsAndSetsMessage() {
            StubWorkload workload = new StubWorkload(false, false, false, Duration.ofSeconds(5), true);
            try (Progressbar pb = new Progressbar(workload)) {
                pb.start("hello");

                ProgressBarBuilder.ProgressBar up = extractUnderlying(pb);
                assertEquals(1, up.getCurrent());
                assertEquals("hello", up.getExtraMessage());
            }
        }

        @Test
        void startNoMessageDoesNotFail() {
            StubWorkload workload = new StubWorkload(true, false, false, Duration.ofSeconds(3), true);
            try (Progressbar pb = new Progressbar(workload)) {
                assertDoesNotThrow(() -> { pb.start(); return null; });
            }
        }
    }

    @Nested
    @DisplayName("step() behavior")
    class StepTests {
        @Test
        void stepAdvancesByElapsedAndSetsRateMessage() throws InterruptedException {
            StubWorkload workload = new StubWorkload(false, false, false, Duration.ofSeconds(3), true);
            workload.setMetrics(123.4, 2, 3);
            try (Progressbar pb = new Progressbar(workload)) {
                pb.start();
                Thread.sleep(1100); // ensure elapsed time passes to produce delta
                ProgressBarBuilder.ProgressBar upBefore = extractUnderlying(pb);
                long currentBefore = upBefore.getCurrent();

                pb.step("note");

                ProgressBarBuilder.ProgressBar up = extractUnderlying(pb);
                assertTrue(up.getCurrent() >= currentBefore, "current should not decrease");
                String msg = up.getExtraMessage();
                assertTrue(msg.contains("OPS:"));
                assertTrue(msg.contains("Pending:"));
                assertTrue(msg.contains("Errors:"));
                assertTrue(msg.contains("note"));
            }
        }
    }

    @Nested
    @DisplayName("stop() behavior")
    class StopTests {
        @Test
        void stopCompletesAndClosesWhenNotAborted() {
            StubWorkload workload = new StubWorkload(false, false, false, Duration.ofSeconds(2), true);
            try (Progressbar pb = new Progressbar(workload)) {
                pb.start();
                pb.stop("done");

                ProgressBarBuilder.ProgressBar up = extractUnderlying(pb);
                assertEquals(up.getMax(), up.getCurrent());
                assertEquals("done", up.getExtraMessage());
                assertTrue(up.isClosed());
            }
        }

        @Test
        void stopNoMessageAllowed() {
            StubWorkload workload = new StubWorkload(false, false, false, Duration.ofSeconds(1), true);
            try (Progressbar pb = new Progressbar(workload)) {
                pb.start();
                assertDoesNotThrow(() -> { pb.stop(); return null; });
            }
        }
    }

    @Nested
    @DisplayName("close() behavior")
    class CloseTests {
        @Test
        void closeIsIdempotent() {
            StubWorkload workload = new StubWorkload(false, false, false, Duration.ofSeconds(1), true);
            try (Progressbar pb = new Progressbar(workload)) {
                pb.close();
                assertDoesNotThrow(() -> { pb.close(); return null; });
                ProgressBarBuilder.ProgressBar up = extractUnderlying(pb);
                assertTrue(up.isClosed());
            }
        }
    }
}

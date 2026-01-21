package com.aerospike;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.predefined.TestRun;

/**
 * Integration-style test that exercises TestRun with WorkloadProviderScheduler
 * using a 5s warmup and a 5s workload.
 */
class TestRunSchedulerTest {

    /** Simple concrete args with controllable fields for scheduler. */
    static class TestArgs extends TinkerBenchArgs {
        TestArgs(Duration durationSeconds) {
            this.duration = durationSeconds;
            this.warmupDuration = durationSeconds;
            this.shutdownTimeout = Duration.ofSeconds(1);
            this.queriesPerSecond = 2;
            this.schedulers = 1;
            this.workers = 1;
            this.errorsAbort = 1;
            this.qpsThreshold = 0;
            this.endQPS = 0;
            this.incrQPS = 0;
            this.appTestMode = true;
            this.backgroundMode = true; // reduces console noise
        }

        @Override
        public Integer call() {
            return 0;
        }
    }

    private TestArgs newArgs(Duration d) {
        return new TestArgs(d);
    }

    @Test
    @DisplayName("Runs QPS sweep 100->200 by 10")
    void qpsSweep100To200By10() {
        Duration duration = Duration.ofSeconds(1);

        for (int qps = 100; qps <= 200; qps += 10) {
            TestArgs args = newArgs(duration);
            args.queriesPerSecond = qps;

            try (WorkloadProviderScheduler scheduler = new WorkloadProviderScheduler(
                    new OpenTelemetryDummy(),
                    duration,
                    qps,
                    false,
                    false,
                    args)) {

                QueryRunnable run = new TestRun(scheduler, null, null);
                scheduler.Start();
                assertTrue(scheduler.awaitTermination(), "qps " + qps + " did not terminate");
                assertEquals(WorkloadStatus.Completed, scheduler.getStatus());
                assertEquals(duration, scheduler.getTargetRunDuration());
                assertTrue(scheduler.getSuccessCount() > 0, "no queries executed at qps " + qps);
                assertEquals("TestRun", run.Name());
                assertEquals(0, scheduler.getErrorCount(), "workload at qps " + qps + " error count should be zero");
            }
        }
    }

    @Test
    @DisplayName("Runs warmup(5s) then workload(5s) to completion")
    void warmupAndWorkloadComplete() {
        Duration phaseDuration = Duration.ofSeconds(2);
        TestArgs args = newArgs(phaseDuration);

        // Warmup phase
        WorkloadProviderScheduler warmup = new WorkloadProviderScheduler(
                new OpenTelemetryDummy(),
                phaseDuration,
                args.queriesPerSecond,
                true,
                false,
                args);
        try (warmup) {
            QueryRunnable warmupRun = new TestRun(warmup, null, null);
            warmup.Start();
            assertTrue(warmup.awaitTermination(), "warmup did not terminate normally");
            assertEquals(WorkloadStatus.Completed, warmup.getStatus());
            assertEquals(phaseDuration, warmup.getTargetRunDuration());
            assertTrue(warmup.getRunningDuration().toSeconds() >= phaseDuration.toSeconds(), "warmup ran too short");
            assertEquals("TestRun", warmupRun.Name());
            assertEquals(0, warmup.getErrorCount(), "warmup error count should be zero");
        }

        // Workload phase
        TestArgs workloadArgs = newArgs(phaseDuration);
        WorkloadProviderScheduler workload = new WorkloadProviderScheduler(
                new OpenTelemetryDummy(),
                phaseDuration,
                workloadArgs.queriesPerSecond,
                false,
                true,
                workloadArgs);
        try (workload) {
            QueryRunnable workloadRun = new TestRun(workload, null, null);
            workload.Start();
            assertTrue(workload.awaitTermination(), "workload did not terminate normally");
            assertEquals(WorkloadStatus.Completed, workload.getStatus());
            assertEquals(phaseDuration, workload.getTargetRunDuration());
            assertTrue(workload.getRunningDuration().toSeconds() >= phaseDuration.toSeconds(), "workload ran too short");
            assertTrue(workload.getSuccessCount() > 0, "no queries executed");
            assertEquals("TestRun", workloadRun.Name());
            assertEquals(0, workload.getErrorCount(), "workload error count should be zero");
        }
    }
}

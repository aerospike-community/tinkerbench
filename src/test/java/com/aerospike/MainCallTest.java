package com.aerospike;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.aerospike.idmanager.dummyManager;

class MainCallTest {

    @Test
    @DisplayName("Main.call runs warmup+workload 2s at 10 QPS in appTestMode")
    void mainCallWarmupAndWorkload() throws Exception {
        Main main = new Main();

        // Configure minimal, in-memory/appTest run
        main.queryNameOrString = "TestRun";
        main.warmupDuration = Duration.ofSeconds(2);
        main.duration = Duration.ofSeconds(2);
        main.shutdownTimeout = Duration.ofSeconds(1);
        main.closeWaitDuration = Duration.ofSeconds(1);
        main.queriesPerSecond = 10;
        main.incrQPS = 5;
        main.endQPS = 20;
        main.qpsThreshold = 0;
        main.errorsAbort = 10;
        main.schedulers = 1;
        main.workers = 1;
        main.agsHosts = new String[] { "localhost" };
        main.port = 8182;
        main.idManager = new dummyManager();
        main.appTestMode = true; // avoid real AGS connections
        main.backgroundMode = true; // reduce console noise
        main.printResult = false;
        main.hdrHistFmt = false;
        main.debug = false;

        Integer rc = main.call();

        assertEquals(0, rc, "main.call should succeed");
        assertFalse(main.errorRun.get(), "Should not error");
        assertFalse(main.abortRun.get(), "Should not abort");
        assertFalse(main.qpsErrorRun.get(), "Should not QPS-error");
        assertTrue(main.terminateRun.get(), "Should have terminated normally");
    }
}

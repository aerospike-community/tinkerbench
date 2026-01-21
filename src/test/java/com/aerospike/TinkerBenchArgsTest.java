package com.aerospike;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import picocli.CommandLine;
import picocli.CommandLine.ParameterException;

/**
 * Focused unit tests for {@link TinkerBenchArgs} covering converters, defaults, and validation rules
 * without touching external systems.
 */
@SuppressWarnings("unused")
class TinkerBenchArgsTest {

    /**
     * Concrete test subclass so picocli can instantiate and wire @Spec fields.
     */
    static class TestArgs extends TinkerBenchArgs {
        @Override
        public Integer call() {
            return 0;
        }
    }

    private TestArgs parseArgs(String... args) {
        TestArgs parsed = new TestArgs();
        CommandLine cmd = new CommandLine(parsed);
        cmd.parseArgs(args);
        return parsed;
    }

    @Nested
    @DisplayName("DurationConverter")
    class DurationConverterTests {
        private final TinkerBenchArgs.DurationConverter converter = new TinkerBenchArgs.DurationConverter();

        @Test
        void parsesIso8601() {
            Duration result = converter.convert("PT1H2M3S");
            assertEquals(Duration.ofHours(1).plusMinutes(2).plusSeconds(3), result);
        }

        @Test
        void parsesHumanReadable() {
            Duration result = converter.convert("1h30s");
            assertEquals(Duration.ofHours(1).plusSeconds(30), result);
        }

        @Test
        void parsesNumericSeconds() {
            Duration result = converter.convert("45");
            assertEquals(Duration.ofSeconds(45), result);
        }

        @Test
        void rejectsInvalidFormat() {
            Exception ex = assertThrows(DateTimeParseException.class, () -> converter.convert("1hour2"));
            assertNotNull(ex);
        }
    }

    @Nested
    @DisplayName("Scheduler/Worker Converters")
    class SchedulerWorkerConverterTests {
        @Test
        void schedulerDefaultsWhenZeroOrNegative() {
            TinkerBenchArgs.SchedulerConverter converter = new TinkerBenchArgs.SchedulerConverter();
            int expected = TinkerBenchArgs.DefaultProvider.DefaultNbrSchedules();
            assertEquals(expected, converter.convert("0"));
            assertEquals(expected, converter.convert("-1"));
        }

        @Test
        void workerDefaultsWhenZeroOrNegative() {
            TinkerBenchArgs.WorkerConverter converter = new TinkerBenchArgs.WorkerConverter();
            int expected = TinkerBenchArgs.DefaultProvider.DefaultNbrWorks();
            assertEquals(expected, converter.convert("0"));
            assertEquals(expected, converter.convert("-5"));
        }
    }

    @Nested
    @DisplayName("FileExistConverter")
    class FileExistConverterTests {
        @Test
        void returnsExistingFile() throws Exception {
            File temp = File.createTempFile("tba", "txt");
            temp.deleteOnExit();
            TinkerBenchArgs.FileExistConverter converter = new TinkerBenchArgs.FileExistConverter();
            File result = converter.convert(temp.getAbsolutePath());
            assertEquals(temp.getAbsolutePath(), result.getAbsolutePath());
        }

        @Test
        void throwsWhenMissing() {
            TinkerBenchArgs.FileExistConverter converter = new TinkerBenchArgs.FileExistConverter();
            Exception ex = assertThrows(FileNotFoundException.class, () -> converter.convert("/definitely/missing/file.txt"));
            assertNotNull(ex);
        }

        @Test
        void returnsNullOnEmpty() throws Exception {
            TinkerBenchArgs.FileExistConverter converter = new TinkerBenchArgs.FileExistConverter();
            assertNull(converter.convert(""));
            assertNull(converter.convert(null));
        }
    }

    @Nested
    @DisplayName("IdManagerConverter")
    class IdManagerConverterTests {
        @Test
        void returnsDummyForNullish() {
            TinkerBenchArgs.IdManagerConverter converter = new TinkerBenchArgs.IdManagerConverter();
            assertTrue(converter.convert("null") instanceof com.aerospike.idmanager.dummyManager);
            assertTrue(converter.convert("dummy") instanceof com.aerospike.idmanager.dummyManager);
        }

        @Test
        void returnsListDummyWhenRequested() {
            TinkerBenchArgs.IdManagerConverter converter = new TinkerBenchArgs.IdManagerConverter();
            com.aerospike.idmanager.dummyManager result = (com.aerospike.idmanager.dummyManager) converter.convert("list");
            assertTrue(result.listManagers);
        }

        @Test
        void instantiatesConcreteManager() {
            TinkerBenchArgs.IdManagerConverter converter = new TinkerBenchArgs.IdManagerConverter();
            assertTrue(converter.convert("IdSampler") instanceof com.aerospike.idmanager.IdSampler);
        }
    }

    @Nested
    @DisplayName("validate() rules")
    class ValidateRules {
        @Test
        void passesWithDefaults() {
            TestArgs args = parseArgs("g.V()", "-q", "10");
            assertDoesNotThrow(args::validate);
        }

        @Test
        void rejectsZeroDuration() {
            TestArgs args = parseArgs("g.V()", "-d", "0s");
            Exception ex = assertThrows(ParameterException.class, args::validate);
            assertNotNull(ex);
        }

        @Test
        void rejectsZeroQpsThresholdAndEndQps() {
            TestArgs args = parseArgs("g.V()", "-qpspct", "0", "-end", "0");
            Exception ex = assertThrows(ParameterException.class, args::validate);
            assertNotNull(ex);
        }

        @Test
        void rejectsIdChainSamplerWithoutSource() {
            TestArgs args = parseArgs("g.V()", "--IdManager", "IdChainSampler");
            Exception ex = assertThrows(ParameterException.class, args::validate);
            assertNotNull(ex);
        }

        @Test
        void rejectsIdChainSamplerWithBothSources() {
            TestArgs args = parseArgs("g.V()", "--IdManager", "IdChainSampler", "-IdQry", "g.V()", "-import", "file.csv");
            Exception ex = assertThrows(ParameterException.class, args::validate);
            assertNotNull(ex);
        }

        @Test
        void acceptsIdChainSamplerWithQueryOnly() {
            TestArgs args = parseArgs("g.V()", "--IdManager", "IdChainSampler", "-IdQry", "g.V().limit(1)");
            assertDoesNotThrow(args::validate);
        }
    }

    @Nested
    @DisplayName("getArguments() formatting")
    class GetArgumentsFormatting {
        @Test
        void includesDefaultsWhenRequested() {
            TestArgs args = parseArgs("g.V()", "-q", "10");
            String[] all = args.getArguments(false);
            assertTrue(Arrays.stream(all).anyMatch(s -> s.contains("--QueriesPerSec: 10")));
            assertTrue(Arrays.stream(all).anyMatch(s -> s.contains("QueryNameOrGremlinString (Position 0): g.V()")));
            assertTrue(Arrays.stream(all).anyMatch(s -> s.contains("--schedulers: 5 (Default)")));
        }

        @Test
        void onlyProvidedWhenRequested() {
            TestArgs args = parseArgs("g.V()", "-q", "10");
            String[] provided = args.getArguments(true);
            assertTrue(Arrays.stream(provided).anyMatch(s -> s.contains("--QueriesPerSec: 10")));
            assertFalse(Arrays.stream(provided).anyMatch(s -> s.contains("--schedulers: 5 (Default)")));
        }
    }
}

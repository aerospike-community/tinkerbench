package com.aerospike;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Optional;

import org.javatuples.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;


@DisplayName("Helpers Utility Class Tests")
class HelpersTest {


    @Nested
    @DisplayName("Print and Println Methods")
    class PrintTests {

        @Test
        @DisplayName("Should print message with text color")
        void testPrintWithTextColor() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream testStream = new PrintStream(baos);
            String message = "Test message";
            Helpers.Print(testStream, message, Helpers.RED);
            
            String output = baos.toString();
            assertTrue(output.contains(Helpers.RED));
            assertTrue(output.contains(message));
            assertTrue(output.contains(Helpers.RESET));
        }

        @Test
        @DisplayName("Should print message with text and background color")
        void testPrintWithTextAndBackgroundColor() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream testStream = new PrintStream(baos);
            String message = "Test message";
            Helpers.Print(testStream, message, Helpers.GREEN, Helpers.BLACK_BACKGROUND);
            
            String output = baos.toString();
            assertTrue(output.contains(Helpers.BLACK_BACKGROUND));
            assertTrue(output.contains(Helpers.GREEN));
            assertTrue(output.contains(message));
            assertTrue(output.contains(Helpers.RESET));
        }

        @Test
        @DisplayName("Should println message with text color")
        void testPrintlnWithTextColor() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream testStream = new PrintStream(baos);
            String message = "Test message";
            Helpers.Println(testStream, message, Helpers.BLUE);
            
            String output = baos.toString();
            assertTrue(output.contains(Helpers.BLUE));
            assertTrue(output.contains(message));
            assertTrue(output.contains(Helpers.RESET));
        }

        @Test
        @DisplayName("Should println message with text and background color")
        void testPrintlnWithTextAndBackgroundColor() {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream testStream = new PrintStream(baos);
            String message = "Test message";
            Helpers.Println(testStream, message, Helpers.YELLOW, Helpers.BLUE_BACKGROUND);
            
            String output = baos.toString();
            assertTrue(output.contains(Helpers.BLUE_BACKGROUND));
            assertTrue(output.contains(Helpers.YELLOW));
            assertTrue(output.contains(message));
            assertTrue(output.contains(Helpers.RESET));
        }
    }

    @Nested
    @DisplayName("removeDuplicates Method")
    class RemoveDuplicatesTests {

        @Test
        @DisplayName("Should remove duplicate elements")
        void testRemoveDuplicates() {
            List<String> input = List.of("apple", "banana", "apple", "cherry", "banana");
            List<String> result = Helpers.removeDuplicates(input, String::valueOf);
            
            assertEquals(3, result.size());
            assertTrue(result.contains("apple"));
            assertTrue(result.contains("banana"));
            assertTrue(result.contains("cherry"));

            input = List.of("banana", "apple", "apple", "cherry", "banana");
            result = Helpers.removeDuplicates(input, String::valueOf);

            assertEquals(3, result.size());
            assertTrue(result.contains("apple"));
            assertTrue(result.contains("banana"));
            assertTrue(result.contains("cherry"));
        }

        @Test
        @DisplayName("Should handle empty list")
        void testRemoveDuplicatesEmptyList() {
            List<String> input = List.of();
            List<String> result = Helpers.removeDuplicates(input, String::valueOf);
            
            assertEquals(0, result.size());
        }

        @Test
        @DisplayName("Should handle list with no duplicates")
        void testRemoveDuplicatesNoDuplicates() {
            List<String> input = List.of("apple", "banana", "cherry");
            List<String> result = Helpers.removeDuplicates(input, String::valueOf);
            
            assertEquals(3, result.size());
        }
    }

    @Nested
    @DisplayName("GetShortClassName Method")
    class GetShortClassNameTests {

        @Test
        @DisplayName("Should extract simple class name from fully qualified name")
        void testGetShortClassName() {
            String result = Helpers.GetShortClassName("com.aerospike.Helpers");
            assertEquals("Helpers", result);
        }

        @Test
        @DisplayName("Should handle null input")
        void testGetShortClassNameNull() {
            String result = Helpers.GetShortClassName(null);
            assertEquals("", result);
        }

        @Test
        @DisplayName("Should handle class name with single part")
        void testGetShortClassNameSinglePart() {
            String result = Helpers.GetShortClassName("Helpers");
            assertEquals("Helpers", result);
        }
    }

    @Nested
    @DisplayName("GetShortErrorMsg Methods")
    class GetShortErrorMsgTests {

        @Test
        @DisplayName("Should get short error message")
        void testGetShortErrorMsg() {
            String errMsg = "java.lang.NullPointerException: Cannot read field";
            String result = Helpers.GetShortErrorMsg(errMsg);
            
            assertNotNull(result);
            assertFalse(result.isEmpty());
        }

        @Test
        @DisplayName("Should handle null error message")
        void testGetShortErrorMsgNull() {
            String result = Helpers.GetShortErrorMsg(null);
            assertEquals("", result);
        }

        @Test
        @DisplayName("Should truncate message to specified length")
        void testGetShortErrorMsgWithLength() {
            String errMsg = "This is a long error message that should be truncated";
            String result = Helpers.GetShortErrorMsg(errMsg, 20);
            
            assertTrue(result.length() <= 20);
        }

        @Test
        @DisplayName("Should get short error message with prefix and break prefix")
        void testGetShortErrorMsgWithPrefixes() {
            String errMsg = "java.lang.Exception: Test error";
            String result = Helpers.GetShortErrorMsg(errMsg, 0, "[ERROR] ", "-");
            
            assertNotNull(result);
            assertTrue(result.contains("[ERROR]"));
        }
    }

    @Nested
    @DisplayName("getErrorMessage Methods")
    class GetErrorMessageTests {

        @Test
        @DisplayName("Should get error message from exception")
        void testGetErrorMessage() {
            Throwable throwable = new RuntimeException("Test exception");
            String result = Helpers.getErrorMessage(throwable);
            
            assertEquals("Test exception", result);
        }

        @Test
        @DisplayName("Should handle null exception")
        void testGetErrorMessageNull() {
            String result = Helpers.getErrorMessage(null);
            assertEquals("<Null>", result);
        }

        @Test
        @DisplayName("Should handle exception with null message")
        void testGetErrorMessageNullMessage() {
            Throwable throwable = new RuntimeException();
            String result = Helpers.getErrorMessage(throwable);
            
            assertNotNull(result);
        }

        @Test
        @DisplayName("Should get error message with depth parameter")
        void testGetErrorMessageWithDepth() {
            Throwable throwable = new RuntimeException("Test exception");
            String result = Helpers.getErrorMessage(throwable, 0);
            
            assertEquals("Test exception", result);
        }
    }

    @Nested
    @DisplayName("GetPid and GetPidThreadId Methods")
    class PidAndThreadIdTests {

        @Test
        @DisplayName("Should get PID")
        void testGetPid() {
            String pid = Helpers.GetPid();
            
            assertNotNull(pid);
            assertFalse(pid.isEmpty());
        }

        @Test
        @DisplayName("Should get PID and Thread ID pair")
        void testGetPidThreadId() {
            Pair<String, String> result = Helpers.GetPidThreadId();
            
            assertNotNull(result);
            assertNotNull(result.getValue0()); // PID
            assertNotNull(result.getValue1()); // Thread ID
        }
    }

    @Nested
    @DisplayName("isNumeric Method")
    class IsNumericTests {

        @Test
        @DisplayName("Should identify integer as numeric")
        void testIsNumericInteger() {
            Pair<Boolean, Boolean> result = Helpers.isNumeric("123");
            
            assertTrue(result.getValue0()); // isNumeric
            assertFalse(result.getValue1()); // hasDecimal
        }

        @Test
        @DisplayName("Should identify float as numeric with decimal")
        void testIsNumericFloat() {
            Pair<Boolean, Boolean> result = Helpers.isNumeric("123.45");
            
            assertTrue(result.getValue0()); // isNumeric
            assertTrue(result.getValue1()); // hasDecimal
        }

        @Test
        @DisplayName("Should identify non-numeric string")
        void testIsNumericString() {
            Pair<Boolean, Boolean> result = Helpers.isNumeric("abc");
            
            assertFalse(result.getValue0());
            assertFalse(result.getValue1());
        }

        @Test
        @DisplayName("Should handle null input")
        void testIsNumericNull() {
            Pair<Boolean, Boolean> result = Helpers.isNumeric(null);
            
            assertFalse(result.getValue0());
        }

        @Test
        @DisplayName("Should handle negative numbers")
        void testIsNumericNegative() {
            Pair<Boolean, Boolean> result = Helpers.isNumeric("-456");
            
            assertTrue(result.getValue0());
            assertFalse(result.getValue1());
        }
    }

    @Nested
    @DisplayName("toProperGremlinObject Method")
    class ToProperGremlinObjectTests {

        @Test
        @DisplayName("Should convert numeric string to Integer")
        void testToProperGremlinObjectInteger() {
            Object result = Helpers.toProperGremlinObject("42");
            
            assertInstanceOf(Integer.class, result);
            assertEquals(42, result);
        }

        @Test
        @DisplayName("Should convert decimal string to Float")
        void testToProperGremlinObjectFloat() {
            Object result = Helpers.toProperGremlinObject("3.14");
            
            assertInstanceOf(Float.class, result);
            assertEquals(3.14f, (Float) result, 0.01f);
        }

        @Test
        @DisplayName("Should convert non-numeric string to quoted string")
        void testToProperGremlinObjectString() {
            Object result = Helpers.toProperGremlinObject("hello");
            
            assertInstanceOf(String.class, result);
            assertTrue(result.toString().startsWith("\""));
        }
    }

    @Nested
    @DisplayName("toProperGremlinString Method")
    class ToProperGremlinStringTests {

        @Test
        @DisplayName("Should wrap string in quotes")
        void testToProperGremlinString() {
            String result = Helpers.toProperGremlinString("test");
            
            assertTrue(result.startsWith("\""));
            assertTrue(result.endsWith("\""));
        }

        @Test
        @DisplayName("Should handle already quoted string")
        void testToProperGremlinStringAlreadyQuoted() {
            String result = Helpers.toProperGremlinString("\"test\"");
            
            assertTrue(result.startsWith("\""));
            assertTrue(result.endsWith("\""));
        }

        @Test
        @DisplayName("Should handle null input")
        void testToProperGremlinStringNull() {
            String result = Helpers.toProperGremlinString(null);
            
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("DetermineValue Methods")
    class DetermineValueTests {

        @Test
        @DisplayName("Should determine integer value")
        void testDetermineValueInteger() {
            Object result = Helpers.DetermineValue("42", "int");
            
            assertInstanceOf(Integer.class, result);
            assertEquals(42, result);
        }

        @Test
        @DisplayName("Should determine float value")
        void testDetermineValueFloat() {
            Object result = Helpers.DetermineValue("3.14", "float");
            
            assertInstanceOf(Float.class, result);
            assertEquals(3.14f, (Float) result, 0.01f);
        }

        @Test
        @DisplayName("Should determine boolean value")
        void testDetermineValueBoolean() {
            Object result = Helpers.DetermineValue("true", "boolean");
            
            assertInstanceOf(Boolean.class, result);
            assertTrue((Boolean) result);
        }

        @Test
        @DisplayName("Should determine string value")
        void testDetermineValueString() {
            Object result = Helpers.DetermineValue("hello", "string");
            
            assertEquals("hello", result);
        }

        @Test
        @DisplayName("Should auto-detect type from value")
        void testDetermineValueAutoDetect() {
            Object resultInt = Helpers.DetermineValue("42");
            Object resultFloat = Helpers.DetermineValue("3.14");
            Object resultBool = Helpers.DetermineValue("true");
            
            assertInstanceOf(Integer.class, resultInt);
            assertInstanceOf(Float.class, resultFloat);
            assertInstanceOf(Boolean.class, resultBool);
        }

        @Test
        @DisplayName("Should handle null or empty input")
        void testDetermineValueNull() {
            Object result = Helpers.DetermineValue(null);
            assertNull(result);
            
            Object result2 = Helpers.DetermineValue("");
            assertEquals("", result2);
        }
    }

    @Nested
    @DisplayName("ReplaceNthOccurrence Method")
    class ReplaceNthOccurrenceTests {

        @Test
        @DisplayName("Should replace first occurrence")
        void testReplaceFirstOccurrence() {
            String result = Helpers.ReplaceNthOccurrence("apple apple cherry", "apple", "banana", 1);
            
            assertEquals("banana apple cherry", result);
        }

        @Test
        @DisplayName("Should replace second occurrence")
        void testReplaceSecondOccurrence() {
            String result = Helpers.ReplaceNthOccurrence("apple apple cherry", "apple", "banana", 2);
            
            assertEquals("apple banana cherry", result);
        }

        @Test
        @DisplayName("Should return original string if occurrence not found")
        void testReplaceOccurrenceNotFound() {
            String result = Helpers.ReplaceNthOccurrence("apple banana cherry", "grape", "fruit", 1);
            
            assertEquals("apple banana cherry", result);
        }

        @Test
        @DisplayName("Should handle invalid occurrence number")
        void testReplaceInvalidOccurrence() {
            String result = Helpers.ReplaceNthOccurrence("apple apple cherry", "apple", "banana", 0);
            
            assertEquals("apple apple cherry", result);
        }
    }

    @Nested
    @DisplayName("TrimTrailingEmptyOrNull Method")
    class TrimTrailingEmptyOrNullTests {

        @Test
        @DisplayName("Should trim trailing empty strings")
        void testTrimTrailingEmpty() {
            String[] input = {"apple", "banana", "", null};
            String[] result = Helpers.TrimTrailingEmptyOrNull(input);
            
            assertEquals(2, result.length);
            assertEquals("apple", result[0]);
            assertEquals("banana", result[1]);

            input = new String[]{"apple", "banana", "", "cherry", null};
            result = Helpers.TrimTrailingEmptyOrNull(input);

            assertEquals(4, result.length);
            assertEquals("apple", result[0]);
            assertEquals("banana", result[1]);
            assertEquals("", result[2]);
            assertEquals("cherry", result[3]);
        }

        @Test
        @DisplayName("Should handle array with no trailing empty strings")
        void testTrimNoTrailingEmpty() {
            String[] input = {"apple", "banana", "cherry"};
            String[] result = Helpers.TrimTrailingEmptyOrNull(input);
            
            assertEquals(3, result.length);
        }

        @Test
        @DisplayName("Should handle null input")
        void testTrimNull() {
            String[] result = Helpers.TrimTrailingEmptyOrNull(null);
            
            assertNull(result);
        }

        @Test
        @DisplayName("Should handle array with all trailing empty")
        void testTrimAllTrailingEmpty() {
            String[] input = {"apple", "", "", null};
            String[] result = Helpers.TrimTrailingEmptyOrNull(input);
            
            assertEquals(1, result.length);
        }
    }

    @Nested
    @DisplayName("Unwrap Method")
    class UnwrapTests {

        @Test
        @DisplayName("Should unwrap Optional with value")
        void testUnwrapOptional() {
            Optional<String> optional = Optional.of("test");
            Object result = Helpers.Unwrap(optional);
            
            assertEquals("test", result);
        }

        @Test
        @DisplayName("Should unwrap empty Optional")
        void testUnwrapEmptyOptional() {
            Optional<String> optional = Optional.empty();
            Object result = Helpers.Unwrap(optional);
            
            assertNull(result);
        }

        @Test
        @DisplayName("Should return non-Optional object unchanged")
        void testUnwrapNonOptional() {
            String value = "test";
            Object result = Helpers.Unwrap(value);
            
            assertEquals("test", result);
        }
    }

    @Nested
    @DisplayName("DateTime Methods")
    class DateTimeTests {

        @Test
        @DisplayName("Should get local time zone")
        void testGetLocalTimeZone() {
            LocalDateTime localDateTime = LocalDateTime.now();
            ZonedDateTime result = Helpers.GetLocalTimeZone(localDateTime);
            
            assertNotNull(result);
        }

        @Test
        @DisplayName("Should get UTC time")
        void testGetUTC() {
            LocalDateTime localDateTime = LocalDateTime.now();
            ZonedDateTime result = Helpers.GetUTC(localDateTime);
            
            assertNotNull(result);
        }

        @Test
        @DisplayName("Should get UTC string representation")
        void testGetUTCString() {
            LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 0);
            String result = Helpers.GetUTCString(localDateTime);
            
            assertNotNull(result);
            assertTrue(result.contains("2024"));
        }

        @Test
        @DisplayName("Should get local time zone string")
        void testGetLocalTimeZoneString() {
            LocalDateTime localDateTime = LocalDateTime.now();
            String result = Helpers.GetLocalTimeZoneString(localDateTime);
            
            assertNotNull(result);
            assertFalse(result.isEmpty());
        }

        @Test
        @DisplayName("Should get local zone string")
        void testGetLocalZoneString() {
            String result = Helpers.GetLocalZoneString();
            
            assertNotNull(result);
            assertFalse(result.isEmpty());
        }

        @Test
        @DisplayName("Should get date time string")
        void testGetDateTimeString() {
            LocalDateTime localDateTime = LocalDateTime.of(2024, 1, 15, 10, 30, 45);
            String result = Helpers.GetDateTimeString(localDateTime);
            
            assertNotNull(result);
            assertTrue(result.contains("2024"));
        }
    }

    @Nested
    @DisplayName("PrintGrafanaRangeJson Method")
    class PrintGrafanaRangeJsonTests {

        @Test
        @DisplayName("Should print Grafana range JSON")
        void testPrintGrafanaRangeJson() {
            LocalDateTime start = LocalDateTime.of(2024, 1, 15, 10, 0, 0);
            LocalDateTime stop = LocalDateTime.of(2024, 1, 15, 11, 0, 0);
            String result = Helpers.PrintGrafanaRangeJson(start, stop);
            
            assertNotNull(result);
            assertTrue(result.contains("Grafana UTC Date Range"));
            assertTrue(result.contains("from"));
            assertTrue(result.contains("to"));
        }

        @Test
        @DisplayName("Should handle null dates")
        void testPrintGrafanaRangeJsonNull() {
            String result = Helpers.PrintGrafanaRangeJson(null, null);
            
            assertNull(result);
        }
    }

    @Nested
    @DisplayName("RoundNumber Methods")
    class RoundNumberTests {

        @Test
        @DisplayName("Should round to significant digits")
        void testRoundNumberOfSignificantDigits() {
            double result = Helpers.RoundNumberOfSignificantDigits(1234.5678, 2);
            
            assertEquals(1234.57, result, "2 Significant Digits");

            result = Helpers.RoundNumberOfSignificantDigits(1234.5678, 0);

            assertEquals(1235.00, result, "0 Significant Digits");

            result = Helpers.RoundNumberOfSignificantDigits(1234.5678, 5);

            assertEquals(1234.5678, result, "0 Significant Digits");
        }

        @Test
        @DisplayName("Should round with scale")
        void testRoundNumberOfSignificantDigitsScale() {
            double result = Helpers.RoundNumberOfSignificantDigitsScale(1234.5678, 100);
            
            assertTrue(result > 0);
        }

        @Test
        @DisplayName("Should handle zero significant digits")
        void testRoundNumberOfSignificantDigitsZero() {
            double result = Helpers.RoundNumberOfSignificantDigits(1234.5678, 0);
            
            assertEquals(1235, result);
        }
    }

    @Nested
    @DisplayName("FmtDuration Method")
    class FmtDurationTests {

        @Test
        @DisplayName("Should format duration with hours")
        void testFmtDurationWithHours() {
            Duration duration = Duration.ofHours(2).plusMinutes(30).plusSeconds(45);
            String result = Helpers.FmtDuration(duration);
            
            assertTrue(result.contains("H"));
            assertTrue(result.contains("M"));
        }

        @Test
        @DisplayName("Should format duration with minutes")
        void testFmtDurationWithMinutes() {
            Duration duration = Duration.ofMinutes(15).plusSeconds(30);
            String result = Helpers.FmtDuration(duration);
            
            assertTrue(result.contains("M"));
            assertTrue(result.contains("S"));
            assertEquals("15M30.000S",  result);
        }

        @Test
        @DisplayName("Should format duration with seconds")
        void testFmtDurationWithSeconds() {
            Duration duration = Duration.ofSeconds(45);
            String result = Helpers.FmtDuration(duration);
            
            assertTrue(result.contains("S"));
            assertEquals("45.000S",  result);
        }

        @Test
        @DisplayName("Should format duration with milliseconds")
        void testFmtDurationWithMilliseconds() {
            Duration duration = Duration.ofMillis(500);
            String result = Helpers.FmtDuration(duration);
            
            assertTrue(result.contains("MS"));
            assertEquals("500MS",  result);
        }
    }

    @Nested
    @DisplayName("FmtInt Method")
    class FmtIntTests {

        @Test
        @DisplayName("Should format large integers with G suffix")
        void testFmtIntBillions() {
            String result = Helpers.FmtInt(1_500_000_000);
            
            assertTrue(result.contains("G"));
            assertEquals("1.500G",   result);
        }

        @Test
        @DisplayName("Should format medium integers with M suffix")
        void testFmtIntMillions() {
            String result = Helpers.FmtInt(1_500_000);
            
            assertTrue(result.contains("M"));
            assertEquals("1.500M",   result);
        }

        @Test
        @DisplayName("Should format thousands with K suffix")
        void testFmtIntThousands() {
            String result = Helpers.FmtInt(1500);
            
            assertTrue(result.contains("K"));
            assertEquals("1.500K",   result);
        }

        @Test
        @DisplayName("Should format small integers without suffix")
        void testFmtIntSmall() {
            String result = Helpers.FmtInt(500);
            
            assertTrue(result.contains("500"));
        }
    }

    @Nested
    @DisplayName("hasWildcard Method")
    class HasWildcardTests {

        @Test
        @DisplayName("Should detect asterisk wildcard")
        void testHasWildcardAsterisk() {
            boolean result = Helpers.hasWildcard("*.txt");
            
            assertTrue(result);
        }

        @Test
        @DisplayName("Should detect question mark wildcard")
        void testHasWildcardQuestionMark() {
            boolean result = Helpers.hasWildcard("file?.txt");
            
            assertTrue(result);
        }

        @Test
        @DisplayName("Should detect no wildcard")
        void testHasNoWildcard() {
            boolean result = Helpers.hasWildcard("file.txt");
            
            assertFalse(result);
        }

        @Test
        @DisplayName("Should handle null input")
        void testHasWildcardNull() {
            boolean result = Helpers.hasWildcard(null);
            
            assertFalse(result);
        }

        @Test
        @DisplayName("Should handle empty input")
        void testHasWildcardEmpty() {
            boolean result = Helpers.hasWildcard("");
            
            assertFalse(result);
        }
    }

    @Nested
    @DisplayName("GetFiles Method")
    class GetFilesTests {

        @Test
        @DisplayName("Should handle null pattern")
        void testGetFilesNullPattern() {
            List<File> result = Helpers.GetFiles(null, null);
            
            assertEquals(0, result.size());
        }

        @Test
        @DisplayName("Should handle empty pattern")
        void testGetFilesEmptyPattern() {
            List<File> result = Helpers.GetFiles(null, "");
            
            assertEquals(0, result.size());
        }
    }

    @Nested
    @DisplayName("CrateFolderFilePath Method")
    class CreateFolderFilePathTests {

        @Test
        @DisplayName("Should return file if path already exists")
        void testCreateFolderFilePathExists() {
            String tempDir = System.getProperty("java.io.tmpdir");
            File result = Helpers.CrateFolderFilePath(tempDir + File.separator + "test.txt");
            
            assertNotNull(result);
        }

        @Test
        @DisplayName("Should create folder path")
        void testCreateFolderFilePath() throws IOException {
            File tempDir = new File(System.getProperty("java.io.tmpdir"), "test_helpers_folder_" + System.nanoTime());
            String filePath = tempDir.getAbsolutePath() + File.separator + "test.txt";
            
            File result = Helpers.CrateFolderFilePath(filePath);
            
            assertNotNull(result);
            
            // Cleanup
            if (tempDir.exists()) {
                tempDir.delete();
            }
        }
    }
}

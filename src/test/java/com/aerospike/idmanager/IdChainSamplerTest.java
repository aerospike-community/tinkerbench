package com.aerospike.idmanager;

import com.aerospike.LogSource;
import com.aerospike.OpenTelemetryDummy;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IdChainSamplerTest {

    final String csvFile = "idChainSamplerTest.csv";
    private IdChainSampler idManager;

    @BeforeEach
    void setUp() {
        idManager = new IdChainSampler();
    }

    @Test
    void enabled() {
        final Integer[][] relationships = {{23, 25, 26}};
        idManager.addPath(relationships);
        assertTrue(this.idManager.enabled());
    }

    @Test
    void checkIdsExists() {
        final Integer[][] relationships = {{23, 214, 215, 216},
                                            {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertTrue(idManager.CheckIdsExists(LogSource.getInstance()));
    }

    @Test
    void getIdCount() {
        final Integer[][] relationships = {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertEquals(7, idManager.getIdCount());
    }

    @Test
    void getStartingIdsCount() {
        final Integer[][] relationships = {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertEquals(1, idManager.getStartingIdsCount());
    }

    @Test
    void getNbrRelationships() {
        final Integer[][] relationships = {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertEquals(6, idManager.getNbrRelationships());
    }

    @Test
    void isEmpty() {
        assertTrue(this.idManager.isEmpty());
        final Integer[][] relationships = {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertFalse(this.idManager.isEmpty());
    }

    @Test
    void reset() {
        final Integer[][] relationships = {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertFalse(idManager.isEmpty());
        idManager.Reset();
        assertFalse(idManager.isEmpty());
    }

    @Test
    void getId() {
        final Integer[][] relationships = {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertEquals(23, idManager.getId());
    }

    @Test
    void getInitialDepth() {
        Integer[][] relationships = new Integer[][]{{23},
                {23}};

        idManager.addPath(relationships);
        assertEquals(0, idManager.getInitialDepth());

        relationships = new Integer[][] {{23, 214, 215, 216},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertEquals(3, idManager.getInitialDepth());

        relationships = new Integer[][]{{23, 214, 215, 216, 217},
                {23, 314, 315, 316}};
        idManager.addPath(relationships);
        assertEquals(4, idManager.getInitialDepth());

        relationships = new Integer[][]{{24, 4214, 4215, 4216, 4217},
                {24, 4314, 4315, 4316}};
        idManager.addPath(relationships);
        assertEquals(4, idManager.getInitialDepth());
    }

    @Test
    void getIds() {
        Integer[][] relationships = {{23, 214, 215, 216, 217},
                {23, 314, 315, 316},
                {315, 326}};
        idManager.addPath(relationships);
        Object[] ids = idManager.getIds();
        assertEquals(5, ids.length, "Number of returned ids are incorrect");
        assertEquals(23,  ids[0], "Not the correct top-level parent");
        assertTrue(ids[1].equals(214) || ids[1].equals(314), "Not the correct child");
        assertTrue(ids[2].equals(215) || ids[2].equals(315),  "Not the correct grandchild");
        assertTrue(ids[3].equals(216) || ids[3].equals(316) || ids[3].equals(326),   "Not the correct great-grandchild");
        assertTrue(ids[4] == null || ids[4].equals(217),  "Not the correct great-great-grandchild");

        relationships =  new Integer[][]{{24, 4214, 4215, 4216, 4217},
                {24, 4314, 4315, 4316},
                {4315, 4326}};
        idManager.addPath(relationships);
        ids = idManager.getIds();
        assertEquals(5, ids.length, "Number of returned ids are incorrect");
        assertTrue(ids[0].equals(23) || ids[0].equals(24), "Not the correct top-level parent");
        assertTrue(ids[1].equals(214) || ids[1].equals(314)
                || ids[1].equals(4214) || ids[1].equals(4314), "Not the correct child");
        assertTrue(ids[2].equals(215) || ids[2].equals(315)
                || ids[2].equals(4215) || ids[2].equals(4315),  "Not the correct grandchild");
        assertTrue(ids[3].equals(216) || ids[3].equals(316) || ids[3].equals(326)
                || ids[3].equals(4216) || ids[3].equals(4316) || ids[3].equals(4326),   "Not the correct great-grandchild");
        assertTrue(ids[4] == null || ids[4].equals(217)
                || ids[4].equals(4217),  "Not the correct great-great-grandchild");

    }

    @Test
    void exportimportFile() {

        {
            final Integer[][] relationships = {{23, 214, 215, 216},
                    {23, 314, 315, 316},
                    {315, 326},
                    {24, 4214, 4215, 4216},
                    {24, 4314, 4315, 4316},
                    {4315, 4326}};
            idManager.addPath(relationships);

            try {
                Files.deleteIfExists(Paths.get(csvFile));
            } catch (IOException ignored) {
            }

            idManager.exportFile(csvFile, LogSource.getInstance());

            Path path = Paths.get(csvFile);

            // Assert that the file exists, providing a clear message if it fails
            assertTrue(Files.exists(path), "File should exist: " + path.toAbsolutePath());

            final String[][] expectedExportedData = new String[][]{
                    {"23"},
                    {"24"},
                    {"23", "214", "215", "216"},
                    {"23", "314", "315", "316"},
                    {"23", "314", "315", "326"},
                    {"24", "4214", "4215", "4216"},
                    {"24", "4314", "4315", "4316"},
                    {"24", "4314", "4315", "4326"}};

            List<String[]> actualData = readCsvFile(csvFile);

            // Compare with expected results
            assertEquals(expectedExportedData.length, actualData.size(),
                    "Number of rows read should match expected data");

            for (int i = 0; i < expectedExportedData.length; i++) {
                assertArrayEquals(expectedExportedData[i], actualData.get(i),
                        "Row " + i + " should match expected data");
            }
        }

        {
            idManager.relationshipGraph.clear();
            assertTrue(idManager.isEmpty(), "Should be empty after clear");

            idManager.importFile(csvFile,
                    new OpenTelemetryDummy(),
                    LogSource.getInstance(),
                    200,
                    null);

            assertFalse(idManager.isEmpty(), "Should not be empty after import");
            assertEquals(2, idManager.getStartingIdsCount(), "Top-Level Parents (starting) is incorrect");
            assertEquals(3, idManager.getInitialDepth(), "Initial Depth is incorrect");
            assertEquals(16, idManager.getIdCount(), "Number of undoes is incorrect");
            assertEquals(14, idManager.getNbrRelationships(), "Number of relationships is incorrect");

            idManager.printStats(LogSource.getInstance());
        }

        {
            idManager.relationshipGraph.clear();
            assertTrue(idManager.isEmpty(), "Should be empty after clear");

            idManager.importFile(csvFile,
                    new OpenTelemetryDummy(),
                    LogSource.getInstance(),
                    7,
                    null);

            assertFalse(idManager.isEmpty(), "Should not be empty after import");
            assertEquals(2, idManager.getStartingIdsCount(), "Top-Level Parents (starting) is incorrect");
            assertEquals(3, idManager.getInitialDepth(), "Depth is incorrect");
            assertEquals(9, idManager.getIdCount(), "Number of undoes is incorrect");
            assertEquals(7, idManager.getNbrRelationships(), "Number of relationships is incorrect");

            idManager.printStats(LogSource.getInstance());
        }

        try {
            Files.deleteIfExists(Paths.get(csvFile));
        }  catch (IOException ignored) {}

    }

    private List<String[]> readCsvFile(String csvFile) {
        try {
            return Files.lines(Path.of(csvFile))
                    .filter(line -> !line.trim().isEmpty()) // Filter out empty lines
                    .map(line -> line.split(","))
                    .toList();
        } catch (IOException e) {
            return  new ArrayList<>();
        }
    }

}
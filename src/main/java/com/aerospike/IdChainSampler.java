package com.aerospike;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

public class IdChainSampler implements  IdManager {

    final Random random = new Random();
    final RelationshipGraph<Object> relationshipGraph;
    // A list of defined Ids for Depth Reference...
    final List<Object> currentIds;
    int requestedDepth = -1;
    int actualDepth = -1;
    boolean disabled = false;

    public IdChainSampler() {
        relationshipGraph = new RelationshipGraph<>();
        currentIds = new ArrayList<Object>();
    }

    /**
     * Determines if Ids have been populated...
     * @param logger The logger source instance
     * @return True if there are Ids available.
     */
    @Override
    public boolean CheckIdsExists(LogSource logger) {
        if(disabled) {
            return false;
        }

        if(relationshipGraph.isEmpty()) {
            String msg = "No Ids returned and at least one Id is required! Maybe you need to disable Id Sampling?";
            logger.Print("IdSampler", true, msg);

            throw new ArrayIndexOutOfBoundsException(msg);
        }
        return true;
    }

    private void clear() {
        relationshipGraph.clear();
        currentIds.clear();
        requestedDepth = -1;
        actualDepth = -1;
    }

    /**
     * @param g
     * @param openTelemetry
     * @param logger
     * @param sampleSize
     * @param labels
     */
    @Override
    public void init(GraphTraversalSource g,
                     OpenTelemetry openTelemetry,
                     LogSource logger,
                     int sampleSize,
                     String[] labels) {

    }

    /**
     * @return true to indicate that the Id Manager has been initialized.
     */
    @Override
    public boolean isInitialized() {
        return !relationshipGraph.isEmpty();
    }

    @Override
    final public void Reset() { currentIds.clear(); }

    /**
     * This will always reset (clear) the current Id list...
     * @return The random Root/Parent Id (Depth 0)
     */
    @Override
    final public Object getId() {
        if(currentIds.size() == 1) {
            return currentIds.getFirst();
        }

        final Set<?> roots = relationshipGraph.getTopLevelParents();
        if (roots.isEmpty()) return null;
        final Object root = Helpers.Unwrap(roots.stream()
                                            .skip(random.nextInt(roots.size()))
                                            .findFirst());
        currentIds.add(root);
        return root;
    }

    /**
     * Obtains a Random Id based on Depth.
     *
     * @param depth The depth to obtain a random child of a predefined parent.
     *              A value of 0, returns the root id
     *              If the parent hasn't been defined, it will be selected.
     *              If the depth has been defined, the same Id is retuned.
     * @return the random child Id at depth based on its parent
     */
    @Override
    final public Object getId(int depth) {
        if(depth < 0) return null;
        if(depth == 0) return getId();
        Object parentId;

        if(currentIds.size() == depth) {
            //Use Last item; should be this child's parent
            parentId = currentIds.getLast();
        } else if (currentIds.size() > depth) {
            // Should already have Child in current id list
            return currentIds.get(depth);
        } else {
            // Determine Missing Parent(s)
            parentId = getId(depth-1);
            //currentIds.add(parentId);
        }

        Object childId = null;
        if(parentId != null) {
            Set<?> children = relationshipGraph.getDirectChildren(parentId);
            if (!children.isEmpty()) {
                childId = Helpers.Unwrap(children
                                            .stream()
                                            .skip(random.nextInt(children.size()))
                                            .findFirst());
            }
        }
        currentIds.add(childId);

        return childId;
    }

    @Override
    public void setDepth(int depth) { this.requestedDepth = depth; }

    @Override
    final public int getDepth() { return requestedDepth; }
    @Override
    final public int getInitialDepth() { return actualDepth; }

    @Override
    final public Object[] getIds() {
        getId(requestedDepth);
        return currentIds.toArray();
    }

    /**
     * The CSV file may contain a header line (starts with '-') or comment lines (starts with #). These lines are ignored.
     *      Each line contains an id with its associated children separated by comma.
     *      An Id can be an integer or string.
     *
     * Example:
     *      23,193,10
     *      23,193,149
     *      23,193,31
     *      23,417,367
     *      23,417,1557
     *      23,2,1092
     *      23,2,1107
     *
     * @param filePath -- A CSV file to be used to import Ids. This Path can contain wildcard chars or be a folder where al CSV files will be imported.
     * @param openTelemetry --The Open Telemetry Instance
     * @param logger -- Logging instance
     * @param sampleSize -- the total number of Distinct Ids imported
     * @param labels -- currently ignored
     * @return the amount of time to import the Ids
     */
    @Override
    public long importFile(final String filePath,
                            final OpenTelemetry openTelemetry,
                            final LogSource logger,
                            final int sampleSize,
                            final String[] labels) {

        if(sampleSize <= 0 || disabled) {
            this.clear();
            disabled = true;
            System.out.println("IdChainSampler is disabled but an import file was supplied. Ignoring importing of file...");
            logger.PrintDebug("IdChainSampler.importFile", "IdChainSampler disabled");
            return 0;
        }

        if(this.relationshipGraph.getTotalDistinctChildCount() >= sampleSize) {
            return 0;
        }

        File file;
        if(Helpers.hasWildcard(filePath)) {
            long latency = 0;
            final List<File> files = Helpers.GetFiles(null, filePath);

            for (File importFile : files) {
               latency += importFile(importFile.getPath(),
                                        openTelemetry,
                                        logger,
                                        sampleSize,
                                        labels);
            }
            if (openTelemetry != null) {
                openTelemetry.setIdMgrGauge("*",
                                            labels,
                                            sampleSize,
                                            this.relationshipGraph.getTotalDistinctChildCount(),
                                            latency);
            }
            return latency;
        } else {
            file = new File(filePath);
            if(file.isDirectory()) {
                File wildCardPath = new File(file, "*.csv");
                return importFile(wildCardPath.getPath(),
                                    openTelemetry,
                                    logger,
                                    sampleSize,
                                    labels);
            }
        }

        if(!file.exists()) {
            logger.Print("IdChainSampler.importFile",
                    true,
                    "File does not exist: " + filePath);
            return 0;
        }

        long startTime = System.currentTimeMillis();
        try (ProgressBar progressBar = new ProgressBarBuilder()
                .setTaskName("Loading ids")
                .hideEta()
                .build();
             CSVReader reader = new CSVReader(new FileReader(file))) {

            progressBar.maxHint(file.length());
            progressBar.setExtraMessage(String.format("Reading File '%s'",file.getName()));

            String[] nextLine;
            final int currentIds = this.relationshipGraph.getTotalDistinctChildCount();

            while ((nextLine = reader.readNext()) != null) {

                final int bytes = Arrays.stream(nextLine) // Create a stream from the array
                                    .mapToInt(String::length) // Map each string to its length
                                    .sum() + nextLine.length + 1; // Sum all the lengths
                if(nextLine.length == 0
                        || nextLine[0].startsWith("#")
                        || nextLine[0].startsWith("-")
                        || nextLine[0].startsWith("/path/")) {
                    progressBar.stepBy(bytes);
                    continue;
                }

                final String[] ids = Helpers.TrimTrailingEmptyOrNull(nextLine);
                if(ids.length == 0) continue;

                this.relationshipGraph.addPath(Arrays.stream(ids)
                                                .map(Helpers::DetermineValue)
                                                .toArray());
                progressBar.stepBy(bytes);
                final int currentTotal =  relationshipGraph.getTotalDistinctChildCount();
                if(currentTotal >= sampleSize) {
                    break;
                }
            }
            progressBar.setExtraMessage(String.format("Loaded %,d Ids from '%s'",
                                        this.relationshipGraph
                                            .getTotalDistinctChildCount()-currentIds,
                                        file.getName()));

            progressBar.refresh();
        } catch (IOException | CsvValidationException e) {
            logger.Print("IdChainSampler.importFile Error in reading CSV file " + file, e);
            throw new RuntimeException(e);
        }

        /*
            Only if it hasn't been previously set
         */
        if(this.getDepth() < 0) {
            this.setDepth(relationshipGraph.getMaxDepthOverall());
        }
        this.actualDepth = relationshipGraph.getMaxDepthOverall();

        long latency = System.currentTimeMillis() - startTime;
        if(openTelemetry != null) {
            openTelemetry.setIdMgrGauge(file.getName(),
                                        labels,
                                        sampleSize,
                                        this.relationshipGraph.getTotalDistinctChildCount(),
                                        latency);
        }

        logger.PrintDebug("IdChainSampler.importFile",
                            "Obtain Samples from '%s' Read %d",
                            file.getAbsolutePath(),
                            this.relationshipGraph.getTotalDistinctChildCount());

        return latency;
    }

    /**
     * @param filePath
     * @param logger
     */
    @Override
    public void exportFile(String filePath,
                           LogSource logger) {

        if(filePath != null && isInitialized() && !disabled) {
            File exportFile = Helpers.CrateFolderFilePath(filePath);

            logger.PrintDebug("IdChainSampler.exportFile",
                                "Writing %d into '%s'",
                                this.getInitialDepth(),
                                exportFile.getAbsolutePath());

            try (ProgressBar progressBar = new ProgressBarBuilder()
                    .setInitialMax(this.relationshipGraph.getTotalDistinctChildCount())
                    .setTaskName("Exporting vertices ids")
                    .hideEta()
                    .build();
                 BufferedWriter writer = new BufferedWriter(new FileWriter(exportFile))) {

                progressBar.setExtraMessage(exportFile.getName());
                progressBar.refresh();

                final Map<Object,List<List<Object>>> ids = this.relationshipGraph
                                                                .getAllTopLevelPaths(3);

                ids.forEach((id, paths) -> {
                   for (List<Object> path : paths) {

                       try {
                           final List<String> idStrings = path.stream()
                                                               .map(Object::toString)
                                                               .toList();
                           writer.write(String.join( ",", idStrings));
                           writer.newLine();
                           progressBar.stepBy(idStrings.size());
                       } catch (IOException e) {
                           logger.Print("IdChainSampler.exportFile Error in exporting file " + filePath, e);
                       }
                   }
                });

                progressBar.refresh();
                logger.PrintDebug("IdChainSampler.exportFile",
                                    "Written %d into '%s'",
                                    actualDepth,
                                    exportFile.getAbsolutePath());
            } catch (IOException e) {
                logger.Print("IdChainSampler.exportFile Error in exporting file " + filePath, e);
            }
        } else if (filePath == null) {
            logger.PrintDebug("IdChainSampler.exportFile", "IdChainSampler file not provided");
        } else if (disabled) {
            logger.PrintDebug("IdChainSampler.exportFile", "IdChainSampler disabled");
        } else {
            logger.Print("IdChainSampler.exportFile", true, "Cannot export Vertex Ids because there are no Ids!");
        }
    }

    @Override
    public String toString() {
        return """
        IdChainSampler {
            Relation Graph:
                %s
            Current Id Tree:
                %s
            Actual Depth    = %d
            Requested Depth = %d
            Disabled        = %s
        }
        """.formatted(
                this.relationshipGraph,
                this.currentIds,
                this.actualDepth,
                this.requestedDepth,
                this.disabled
        );
    }
}

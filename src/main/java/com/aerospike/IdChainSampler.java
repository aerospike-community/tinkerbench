package com.aerospike;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class IdChainSampler implements  IdManager {

    final Random random = new Random();
    final RelationshipGraph<Object> relationshipGraph;
    // A list of defined Ids for Depth Reference...
    final List<Object> currentIds;
    int requestedDepth = 0;
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
        requestedDepth = 0;
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
            currentIds.add(parentId);
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
     * @param filePath
     * @param openTelemetry
     * @param logger
     * @param sampleSize -- the number of root ids imported
     * @param labels -- currently ignored
     * @return the amount of time to import the Ids
     */
    @Override
    public long importFile(String filePath,
                            OpenTelemetry openTelemetry,
                            LogSource logger,
                            int sampleSize,
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

            try (ProgressBar progressBar = new ProgressBarBuilder()
                    .setTaskName("Loading vertices ids")
                    .hideEta()
                    .build()) {
                final List<File> files = Helpers.GetFiles(null, filePath);
                progressBar.maxHint(files.size());

                for (File importFile : files) {
                    progressBar.setExtraMessage("Reading File " + importFile.getName());
                    progressBar.step();
                    latency += importFile(importFile.getPath(),
                                            openTelemetry,
                                            logger,
                                            sampleSize,
                                            labels);
                }
                progressBar.setExtraMessage(String.format("Loaded %,d Ids",
                                                            this.relationshipGraph.getTotalDistinctChildCount()));
                if (openTelemetry != null) {
                    openTelemetry.setIdMgrGauge("*",
                                                labels,
                                                sampleSize,
                                                this.relationshipGraph.getTotalDistinctChildCount(),
                                                latency);
                }
                progressBar.refresh();
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
        try (CSVReader reader = new CSVReader(new FileReader(file))) {
            String[] nextLine;
            while ((nextLine = reader.readNext()) != null) {
                if(nextLine.length == 0
                        || nextLine[0].startsWith("#")
                        || nextLine[0].startsWith("-")
                        || nextLine[0].startsWith("/path/")) {
                    continue;
                }

                final String[] ids = Helpers.TrimTrailingEmptyOrNull(nextLine);
                if(ids.length == 0) continue;

                this.relationshipGraph.addPath(Arrays.stream(ids)
                                                .map(Helpers::DetermineValue)
                                                .toArray());
                if(this.relationshipGraph.getTotalDistinctChildCount() >= sampleSize) {
                    break;
                }
            }
        } catch (IOException | CsvValidationException e) {
            logger.Print("IdChainSampler.importFile Error in reading CSV file " + file, e);
            throw new RuntimeException(e);
        }

        this.setDepth(relationshipGraph.getMaxDepthOverall());
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

    }
}

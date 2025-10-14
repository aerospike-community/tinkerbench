package com.aerospike;

import com.opencsv.exceptions.CsvValidationException;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.io.*;
import java.util.*;
import java.util.concurrent.CompletionException;

import com.opencsv.CSVReaderHeaderAware;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.id;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.label;

public class IdSampler implements  IdManager {
    private List<Map<String,Object>> sampledIds = null;
    final Random random = new Random();
    boolean disabled = false;
    String[] labels = null;

    public IdSampler() {
        // Constructor can be used for initialization if needed
    }

    @Override
    public boolean CheckIdsExists(final LogSource logger) {

        if(disabled) {
            return false;
        }

        if(sampledIds.isEmpty()) {
            String msg = labels == null || labels.length == 0
                    ? "No Vertex Ids returned and at least one Id is required! Maybe you need to disable Id Sampling?"
                    : String.format("No Vertex Ids returned for label(s) '%s'. At least one Id is required! Is this label correct or maybe you need to disable Id Sampling?",
                                        Arrays.toString(labels));
            logger.Print("IdSampler", true, msg);

            throw new ArrayIndexOutOfBoundsException(msg);
        }
        return true;
    }

    private void DetermineLabels(String[] useLabels) {
        if(useLabels == null
            || useLabels.length == 0
            || (useLabels.length == 1
                && (useLabels[0].isEmpty()
                    || useLabels[0].equalsIgnoreCase("null")))) {
            this.labels = null;
        } else {
            this.labels = useLabels;
        }
    }

    private static GraphTraversal<?, Map<String,Object>> hasLabel(final GraphTraversalSource g,
                                                 final String[] labels) {
       if(labels == null || labels.length == 0) {
           return g.V()
                   .project("id", "label")
                   .by(id()).by(label());
       }

       if(labels.length == 1) {
           return g.V()
                   .hasLabel(labels[0])
                   .project("id", "label")
                   .by(id()).by(label());
       }

        return g.V()
                .hasLabel(labels[0],
                            Arrays.copyOfRange(labels, 1, labels.length))
                .project("id", "label")
                .by(id()).by(label());
    }

    private static List<Map<String,Object>> getSampledIds(final GraphTraversalSource g,
                                              final String[] labels,
                                              final int sampleSize,
                                              final int start,
                                              final int end) {

        if(start == 0 && end == 0) {
            return hasLabel(g, labels)
                    .limit(sampleSize)
                    .toList();
        }

        return hasLabel(g, labels)
                    .range(start, end)
                    .toList();
    }

    @Override
    public void init(final GraphTraversalSource g,
                     final OpenTelemetry openTelemetry,
                     final LogSource logger,
                     final int sampleSize,
                     final String[] useLabels) {

        long start = 0;
        long end;

        DetermineLabels(useLabels);

        if(sampleSize <= 0) {
            sampledIds = null;
            disabled = true;
            logger.PrintDebug("IdSampler", "IdSampler disabled");
        } else {
            try {
                if(labels == null || labels.length == 0) {
                    System.out.println("Obtaining Vertices Ids...");
                    logger.info("Obtaining Vertices Ids...");
                } else {
                    System.out.printf("Obtaining Vertices Ids for Label(s) '%s'...%n ",
                                        Arrays.toString(labels));
                    logger.info("Obtaining Vertices Ids for Label(s) '{}'...",
                            Arrays.toString(labels));
                }

                logger.PrintDebug("IdSampler",
                                    "Trying to obtain Samples: Label(s): '%s'",
                                    Arrays.toString(labels));

                start = System.currentTimeMillis();
                sampledIds = getSampledIds(g, labels, sampleSize, 0, 0);
                end = System.currentTimeMillis();
            } catch (CompletionException ignored) {
                //TODO: Really need to rework this to avoid AGS exceptions around large sample sizes...
                System.out.printf("Retrying Id Sample Size of %,d\n", sampleSize);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored2) {
                }
                final int portion = sampleSize / 10;
                List<Map<String,Object>> portionLst = getSampledIds(g,
                                                                    labels, sampleSize,
                                                                    0, portion);
                sampledIds = new ArrayList<>(sampleSize);
                sampledIds.addAll(portionLst);

                for (int i = 1; i < 10; i++) {
                    final int startRange = portion * i;
                    final int endRange = startRange + portion;
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored2) {
                    }
                    portionLst = getSampledIds(g, labels, sampleSize,
                                                startRange, endRange);
                    sampledIds.addAll(portionLst);
                }
                end = System.currentTimeMillis();
            }

            logger.PrintDebug("IdSampler",
                                "Obtain Samples: Label(s): '%s' Count: %d",
                                Arrays.toString(labels),
                                sampledIds.size());

            final long runningLatency = end - start;
            if(openTelemetry != null) {
                openTelemetry.setIdMgrGauge(this.getClass().getSimpleName(),
                                            labels,
                                            sampleSize,
                                            sampledIds.size(),
                                            runningLatency);
            }

            logger.info("Completed retrieving Ids ({}) for label(s) '{}' in {} ms",
                            sampledIds.size(),
                            Arrays.toString(labels),
                            runningLatency);
            System.out.println("\tCompleted");
        }
    }

    @Override
    public Object getId() {
        return sampledIds == null
                ? null
                : sampledIds.get(random.nextInt(sampledIds.size())).get("id");
    }

    @Override
    public boolean isInitialized() {
        return sampledIds != null && !sampledIds.isEmpty();
    }

    @Override
    public long importFile(final String filePath,
                               final OpenTelemetry openTelemetry,
                               final LogSource logger,
                               final int sampleSize,
                               final String[] useLabels) {

        if(sampleSize <= 0 || disabled) {
            sampledIds = null;
            disabled = true;
            System.out.println("IdSampler is disabled but an import file was supplied. Ignoring importing of file...");
            logger.PrintDebug("IdSampler.importFile", "IdSampler disabled");
            return 0;
        }

        if(sampledIds == null) {
            sampledIds = new ArrayList<>();
        } else if(sampledIds.size() >= sampleSize) {
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
                progressBar.setExtraMessage(String.format("Loaded %,d vertices", sampledIds.size()));
                if (openTelemetry != null) {
                    openTelemetry.setIdMgrGauge("*",
                                                    labels,
                                                    sampleSize,
                                                    sampledIds.size(),
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
            logger.Print("IdSampler.importFile",
                            true,
                        "File does not exist: " + filePath);
            return 0;
        }
        DetermineLabels(useLabels);

        long startTime = System.currentTimeMillis();
        try (CSVReaderHeaderAware reader = new CSVReaderHeaderAware(new FileReader(file))) {
            Map<String, String> row;
            while ((row = reader.readMap()) != null) {
                // Access data using header names
                String idValue = row.get("-id");
                String labelValue = row.get("-label");

                if(labels == null
                        || Arrays.asList(labels).contains(labelValue)) {
                    if(labelValue == null) {
                        sampledIds.add(Map.of("id", Helpers.DetermineValue(idValue)));
                    } else {
                        sampledIds.add(Map.of("id", Helpers.DetermineValue(idValue),
                                                "label", Helpers.DetermineValue(labelValue)));
                    }
                }
                if(sampledIds.size() >= sampleSize) {
                    break;
                }
            }
        } catch (IOException | CsvValidationException e) {
            logger.Print("IdSampler.importFile Error in reading CSV file " + file, e);
            throw new RuntimeException(e);
        }

        long latency = System.currentTimeMillis() - startTime;
        if(openTelemetry != null) {
            openTelemetry.setIdMgrGauge(file.getName(),
                                        labels,
                                        sampleSize,
                                        sampledIds.size(),
                                        latency);
        }

        logger.PrintDebug("IdSampler.importFile",
                        "Obtain Samples from '%s' using Label(s) '%s' Read %d",
                            file.getAbsolutePath(),
                            Arrays.toString(labels),
                            sampledIds.size());

        return latency;
    }

    @Override
    public void exportFile(final String filePath,
                           final LogSource logger) {

        if(filePath != null && isInitialized() && !disabled) {
            File exportFile = Helpers.CrateFolderFilePath(filePath);
            String header = "-id,-label";

            logger.PrintDebug("IdSampler.exportFile",
                                "Writing %d into '%s'",
                                        sampledIds.size(),
                                        exportFile.getAbsolutePath());

            try (ProgressBar progressBar = new ProgressBarBuilder()
                                                .setInitialMax(sampledIds.size())
                                                .setTaskName("Exporting vertices ids")
                                                .hideEta()
                                                .build();
                    BufferedWriter writer = new BufferedWriter(new FileWriter(exportFile))) {

                progressBar.setExtraMessage(exportFile.getName());

                // Write the header
                writer.write(header);
                writer.newLine(); // Add a new line after the header

                for(Map<String,Object> idmap : sampledIds) {
                    progressBar.step();
                    writer.write(idmap.get("id").toString());
                    writer.write(",");
                    if(idmap.get("label") != null) {
                        writer.write(idmap.get("label").toString());
                    }
                    writer.newLine();
                }

                progressBar.refresh();
                logger.PrintDebug("IdSampler.exportFile",
                                    "Written %d into '%s'",
                                    sampledIds.size(),
                                    exportFile.getAbsolutePath());
            } catch (IOException e) {
                logger.Print("IdSampler.exportFile Error in exporting file " + filePath, e);
            }
        } else if (filePath == null) {
            logger.PrintDebug("IdSampler.exportFile", "IdSampler file not provided");
        } else if (disabled) {
            logger.PrintDebug("IdSampler.exportFile", "IdSampler disabled");
        } else {
            logger.Print("IdSampler.exportFile", true, "Cannot export Vertex Ids because there are no Ids!");
        }
    }
}

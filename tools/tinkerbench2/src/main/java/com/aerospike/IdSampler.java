package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MatchStep;

import java.util.*;
import java.util.concurrent.CompletionException;

public class IdSampler implements  IdManager {
    private static final int ID_SAMPLE_SIZE = 500_000;
    private List<Object> sampledIds = null;
    final Random random = new Random();

    public IdSampler() {
        // Constructor can be used for initialization if needed
    }

    private static List<Object> getSampledIds(final GraphTraversalSource g,
                                                final String label,
                                                final int sampleSize,
                                                final int start,
                                                final int end) {
        if(start == 0 && end == 0) {
            return label == null || label.isEmpty()
                    ? g.V()
                        .limit(sampleSize)
                        .id()
                        .toList()
                    : g.V()
                        .hasLabel(label)
                        .id()
                        .limit(sampleSize)
                        .toList();
        }

        return label == null || label.isEmpty()
                ? g.V()
                    .range(0, end)
                    .id()
                    .toList()
                : g.V()
                    .hasLabel(label)
                    .id()
                    .range(0, end)
                    .toList();
    }

    @Override
    public void init(final GraphTraversalSource g,
                     final OpenTelemetry openTelemetry,
                     final LogSource logger,
                     final int sampleSize,
                     final String label) {

        long start = 0;
        long end = 0;

        if(sampleSize <= 0) {
            sampledIds = null;
            logger.PrintDebug("IdSampler", "IdSampler disabled");
        } else {
            try {
                if(label == null || label.isEmpty()) {
                    System.out.println("Obtaining Vertices Ids...");
                    logger.info("Obtaining Vertices Ids...");
                } else {
                    System.out.printf("Obtaining Vertices Ids for Label '%s'...%n ", label);
                    logger.info("Obtaining Vertices Ids for Label '{}'...", label);
                }

                logger.PrintDebug("IdSampler", "Trying to obtain Samples: Label: '%s'", label);

                start = System.currentTimeMillis();
                sampledIds = getSampledIds(g, label, sampleSize, 0, 0);
                end = System.currentTimeMillis();
            } catch (CompletionException ignored) {
                //TODO: Really need to rework this to avoid AGS exceptions around large sample sizes...
                System.out.printf("Retrying Id Sample Size of %,d\n", sampleSize);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored2) {
                }
                final int portion = sampleSize / 10;
                List<Object> portionLst = getSampledIds(g, label, sampleSize,
                                                    0, portion);
                sampledIds = new ArrayList<Object>(sampleSize);
                sampledIds.addAll(portionLst);

                for (int i = 1; i < 10; i++) {
                    final int startRange = portion * i;
                    final int endRange = startRange + portion;
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored2) {
                    }
                    portionLst = getSampledIds(g, label, sampleSize,
                                                startRange, endRange);
                    sampledIds.addAll(portionLst);
                }
                end = System.currentTimeMillis();
            }
            logger.PrintDebug("IdSampler", "Obtain Samples: Label: '%s' Count: %d", label, sampledIds.size());

            if(sampledIds.isEmpty()) {
                String msg = label == null || label.isEmpty()
                                ? "No Vertex Ids returned and at least one Id is required! Maybe you need to disable Id Sampling?"
                                : String.format("No Vertex Ids returned for label '%s'. At least one Id is required! Is this label correct or maybe you need to disable Id Sampling?",
                                                label);
                logger.Print("IdSampler", true, msg);

                throw new ArrayIndexOutOfBoundsException(msg);
            }

            final long runningLatency = end - start;
            if(openTelemetry != null) {
                openTelemetry.setIdMgrGauge(this.getClass().getSimpleName(),
                                            label,
                                            sampleSize,
                                            sampledIds.size(),
                                            runningLatency);
            }

            logger.info("Completed retrieving Ids ({}) for label '{}' in {} ms",
                            sampledIds.size(),
                            label,
                            runningLatency);
            System.out.println("\tCompleted");
        }
    }

    @Override
    public Object getId() {
        return sampledIds == null
                ? null
                : sampledIds.get(random.nextInt(sampledIds.size()));
    }

    @Override
    public boolean isInitialized() {
        return sampledIds != null && !sampledIds.isEmpty();
    }
}

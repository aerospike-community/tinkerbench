package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.*;
import java.util.concurrent.CompletionException;

public class IdSampler implements  IdManager {
    private static final int ID_SAMPLE_SIZE = 500_000;
    private List<Object> sampledIds = null;
    final Random random = new Random();

    public IdSampler() {
        // Constructor can be used for initialization if needed
    }

    @Override
    public void init(GraphTraversalSource g, int sampleSize) {

        try {
            sampledIds = g.V().limit(sampleSize).id().toList();
        } catch (CompletionException ignored) {
            //TODO: Really need to rework this to avoid AGS exceptions around large sample sizes...
            System.out.printf("Retrying Id Sample Size of %,d\n", sampleSize);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored2) {}
            final int portion = sampleSize / 10;
            List<Object> portionLst = g.V().range(0, portion).id().toList();

            sampledIds = new ArrayList<Object>(sampleSize);
            sampledIds.addAll(portionLst);

            for (int i = 1; i < 10; i++) {
                final int startRange = portion * i;
                final int endRange = startRange + portion;
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored2) {}
                portionLst = g.V().range(startRange, endRange).id().toList();
                sampledIds.addAll(portionLst);
            }
        }

    }

    @Override
    public void init(GraphTraversalSource g) {
        init(g, ID_SAMPLE_SIZE);
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

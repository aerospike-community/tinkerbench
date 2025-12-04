package com.aerospike;

import com.opencsv.exceptions.CsvValidationException;
import com.opencsv.CSVReaderHeaderAware;
import me.tongfei.progressbar.ProgressBar;
import me.tongfei.progressbar.ProgressBarBuilder;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class IdChainSampler implements  IdManager {

    final Random random = new Random();
    final HierarchyManager hierarchyManager;
    // A list of defined Ids for Depth Reference...
    final List<Object> currentIds;
    int maxDepth = 0;
    boolean disabled = false;

    public IdChainSampler() {
        hierarchyManager = new HierarchyManager();
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

        if(hierarchyManager.isEmpty()) {
            String msg = "No Ids returned and at least one Id is required! Maybe you need to disable Id Sampling?";
            logger.Print("IdSampler", true, msg);

            throw new ArrayIndexOutOfBoundsException(msg);
        }
        return true;
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
        return !hierarchyManager.isEmpty();
    }

    /**
     * This will always reset (clear) the current Id list...
     * @return The random Root/Parent Id (Depth 0)
     */
    @Override
    final public Object getId() {
        currentIds.clear();
        final List<?> roots = hierarchyManager.getRootNodes();
        if (roots.isEmpty()) return null;
        final Object root = roots.get(random.nextInt(roots.size()));
        currentIds.add(root);
        return roots;
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
            List<?> children = hierarchyManager.getDirectChildren(parentId);
            if (!children.isEmpty()) {
                childId = children.get(random.nextInt(children.size()));
            }
        }
        currentIds.add(childId);

        return childId;
    }

    @Override
    final public void setDepth(int maxDepth) { this.maxDepth = maxDepth; }

    @Override
    final public int getDepth() { return maxDepth; }

    @Override
    final public Object[] getIds() {
        getId(maxDepth);
        return currentIds.toArray();
    }

    /**
     * @param file
     * @param openTelemetry
     * @param logger
     * @param sampleSize
     * @param labels
     * @return
     */
    @Override
    public long importFile(String file,
                           OpenTelemetry openTelemetry,
                           LogSource logger,
                           int sampleSize,
                           String[] labels) {
        return 0;
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

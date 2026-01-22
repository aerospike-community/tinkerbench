package com.aerospike.idmanager;

import com.aerospike.*;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferencePath;

import javax.script.Bindings;
import java.io.*;
import java.util.*;
import java.util.stream.Stream;

public class IdChainSampler implements IdManagerQuery {

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

    /*
     *   @param relationships an array of pairs where the first item is the parent and reminding items are the children.
     */
    public <T> IdChainSampler(T[][] relationships) {
        this();

        for (T[] relationship : relationships) {
            if(relationship == null || relationship.length == 0) {
                continue;
            }
            if(relationship.length == 1) {
                this.relationshipGraph.markAsTopLevelParent(relationship[0]);
            } else {
                this.relationshipGraph.addPath(relationship);
            }
        }
        this.relationshipGraph.syncStructuralTopLevelParentsToMarked();
    }

    @Override
    public boolean enabled() { return !disabled; }

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

        if(this.isEmpty()) {
            String msg = "Error: No Ids Found. If the Ids were imported check your CSV file for proper format ?.";
            logger.Print("IdChainSampler", true, msg);

            throw new ArrayIndexOutOfBoundsException(msg);
        }
        if(this.getStartingIdsCount() == 0) {
           String msg = """
  Error: No Starting Ids (top-level parents) found.
    If the import/query produces circular references, you must define these starting id(s).
        For a query, use the 'startid' label.
        For importing (CSV file), add a Starting Id value(s) to the file. One value per line.""";

            logger.Print("IdChainSampler", true, msg);

            throw new MissingResourceException(msg, "IdChainSampler", "startid");
        }

        return true;
    }

    private void clear() {
        relationshipGraph.clear();
        currentIds.clear();
        requestedDepth = -1;
        actualDepth = -1;
    }

    private int addNode(final List<?> items) {
        final Object[] idArray = items.toArray();
        this.relationshipGraph.addPath(idArray);
        return idArray.length;
    }
    private int addNode(final ReferencePath rp) {
        final Object[] idArray = rp.objects().toArray();
        this.relationshipGraph.addPath(idArray);
        return idArray.length;
    }
    private void addNode(final AbstractMap<?,?> items,
                        final ProgressBarBuilder.ProgressBar progressBar) {

        if(items.containsKey("startId")) {
            this.relationshipGraph.markAsTopLevelParent(items.get("startId"));
            progressBar.step();
        } else if(items.containsKey("startid")) {
            this.relationshipGraph.markAsTopLevelParent(items.get("startid"));
            progressBar.step();
        }
        if(items.containsKey("paths")) {
            Object paths = items.get("paths");
            if(paths instanceof List<?> path) {
                for(Object item : path) {
                    if(item instanceof ReferencePath rp) {
                        progressBar.stepBy(this.addNode(rp));
                    } else if(item instanceof List<?> li) {
                        for (Object i : li) {
                            if (i instanceof ReferencePath rp) {
                                progressBar.stepBy(this.addNode(rp));
                            }
                        }
                    }
                }
            }
        } else if(items.containsKey("path")) {
            Object pathValue = items.get("path");
            if(pathValue instanceof ReferencePath rp) {
                progressBar.stepBy(this.addNode(rp));
            } else {
                throw  new IllegalArgumentException(String.format("Path %s is not a referencePath. Maybe use the 'paths' structure?", pathValue));
            }
        }
    }

    private void populateFromGraphDB(final Stream<?> stream,
                                         final ProgressBarBuilder.ProgressBar progressBar) {
        stream.forEach(item -> {
            if (item instanceof AbstractMap<?,?> map) {
                addNode(map, progressBar);
            } else if (item instanceof List<?> a) {
                progressBar.stepBy(addNode(a));
            } else if (item instanceof ReferencePath rp) {
                progressBar.stepBy(addNode(rp));
            } else {
                progressBar.setExtraMessage("Item Cast Error... Skipping Id...");
            }
        });
    }

    public final <T> void addPath(T[][] relationships) {

        if(relationships == null || relationships.length == 0) {
            return;
        }

        for (T[] relationship : relationships) {
            if(relationship == null || relationship.length == 0) {
                continue;
            }
            if(relationship.length == 1) {
                this.relationshipGraph.markAsTopLevelParent(relationship[0]);
            } else {
                this.relationshipGraph.addPath(relationship);
            }
        }
        this.relationshipGraph.syncStructuralTopLevelParentsToMarked();

        this.currentIds.clear();
        this.actualDepth = this.relationshipGraph.getMaxDepthOverall();
        this.disabled = false;
    }

    /**
     * @param ags -- graph traversal instance
     * @param openTelemetry -- Open Telemetry instance
     * @param logger -- Logger instance
     * @param gremlinString -- Labels
     */
    @Override
    public void init(final AGSGraphTraversal ags,
                     final OpenTelemetry openTelemetry,
                     final LogSource logger,
                     final String gremlinString) {

        logger.PrintDebug("IdChainSampler", "Getting GremlinLangScriptEngine Engine");

        long start = 0;
        long end;

        String gremlin = gremlinString.replace("'", "\"");
        final EvalQueryWorkloadProvider.Terminator gremlinStep
                = EvalQueryWorkloadProvider.DetermineTerminator(gremlin);

        if(gremlinStep == EvalQueryWorkloadProvider.Terminator.none) {
            gremlin = gremlin + ".toList()";
        }
        final String[] parts = gremlin.split("\\.");
        final GremlinLangScriptEngine  engine = new GremlinLangScriptEngine();

        logger.PrintDebug("IdChainSampler", "Binding to " + parts[0]);
        final Bindings bindings = engine.createBindings();
        bindings.put(parts[0], ags.G());

        Helpers.Println(System.out,
                        String.format("Obtaining Ids using Query '%s'",
                                        gremlin),
                        Helpers.BLACK,
                        Helpers.GREEN_BACKGROUND);
        logger.info(String.format("Using IdChainSampler manager. Obtaining Ids using Query '%s'",
                                    gremlin));
        try (ProgressBarBuilder.ProgressBar progressBar = ProgressBarBuilder.Builder()
                                                            .setTaskName("Obtaining Ids...")
                                                            .hideEta()
                                                            .build()) {
            progressBar.setExtraMessage("Querying DB...");
            progressBar.step();
            start = System.currentTimeMillis();
            Object result = engine.eval(gremlin, bindings);
            progressBar.step();
            progressBar.setExtraMessage("Populating Id Manager...");
            if(result instanceof ArrayList<?> arrayLst) {
                populateFromGraphDB(arrayLst.stream(), progressBar);
            } else if(result instanceof HashSet<?> set) {
                populateFromGraphDB(set.stream(), progressBar);
            } else {
                throw new InvalidClassException(result.getClass().getName(),
                                                    "IdChainSampler Query returned an unexpected result. Must terminate with 'toSet' or 'toList'.");
            }
            end = System.currentTimeMillis();
            this.relationshipGraph.syncStructuralTopLevelParentsToMarked();
            final int totalCnt = this.relationshipGraph.getTotal();
            this.actualDepth = relationshipGraph.getMaxDepthOverall();

            progressBar.refresh();
            if(totalCnt == 0) {
                throw new InvalidClassException("Query returned no Ids...");
            }
            System.out.println();
            Helpers.Println(System.out,
                            String.format("Loaded %,d distinct Ids from Query.",
                                            totalCnt),
                            Helpers.BLACK,
                            Helpers.GREEN_BACKGROUND);
            logger.info(String.format("IdChainSampler: Loaded %,d distinct Ids from Query.",
                                        totalCnt));
        } catch (Exception e) {
            System.err.printf("ERROR: could not evaluate gremlin Id script \"%s\". Error: %s\n",
                    gremlin,
                    e.getMessage());
            logger.error(String.format("ERROR: could not evaluate gremlin Id script \"%s\". Error: %s\n",
                            gremlin,
                            e.getMessage()),
                    e);
            throw new RuntimeException(e);
        }

        final long runningLatency = end - start;
        if(openTelemetry != null) {
            openTelemetry.setIdMgrGauge(this.getClass().getSimpleName(),
                                    null,
                                    gremlin,
                                    this.getIdCount(),
                                    this.getStartingIdsCount(),
                                    this.getDepth() + 1,
                                    this.getInitialDepth() + 1,
                                    this.getNbrRelationships(),
                                    runningLatency);
        }
    }

    /**
     * @return true to indicate that the Id Manager has been initialized.
     */
    @Override
    public boolean isInitialized() {
        return !relationshipGraph.isEmpty();
    }

    /*
     *   @return The total number of distinct ids
     */
    @Override
    final public int getIdCount() {
        return this.relationshipGraph.getTotal();
    };
    /*
     *   @return The total number of Starting Ids (root/parents).
     */
    @Override
    public final int getStartingIdsCount() {
        return this.relationshipGraph.getTopLevelParentCount();
    }

    /*
     *   @return The number of relationships between ids
     */
    @Override
    public int getNbrRelationships() { return this.relationshipGraph.getRelationshipCount(); }

    @Override
    public final boolean isEmpty() {
        return this.relationshipGraph.isEmpty();
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

    /*
        @param depth -- The depth from the root node.
                        Zero is the root node, 1 is the child, etc...
                        -1 indicates to use the initial loaded depth.
     */
    @Override
    public void setDepth(int depth) { this.requestedDepth = depth; }

    /*
        @return Zero based depth (zero is the root node)
            as requested by the setDepth function or the actual depth loaded (initial depth).
     */
    @Override
    final public int getDepth() { return requestedDepth < 0 ? this.getInitialDepth() : requestedDepth; }

    /*
        @return Zero based depth (zero is the root node) as load from the DB or File
     */
    @Override
    final public int getInitialDepth() { return actualDepth; }

    @Override
    final public Object[] getIds() {
        getId(getDepth());
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
     * @param sampleSize -- the total number of Distinct Ids imported. If negative id manager is disabled.
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
            openTelemetry.setIdMgrGauge(null, null, null, -1, -1, 0, 0, 0, 0);
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
                                            null,
                                            this.getIdCount(),
                                            this.getStartingIdsCount(),
                                            this.getDepth() + 1,
                                            this.getInitialDepth() + 1,
                                            this.getNbrRelationships(),
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
            final FileNotFoundException fnfe = new FileNotFoundException(filePath);
            logger.error("IdChainSampler.importFile File does not exist: " + filePath, fnfe);
            Helpers.Println(System.err,
                        "Id File does not exist: " + filePath,
                            Helpers.BLACK,
                            Helpers.RED_BACKGROUND);
            throw new RuntimeException(fnfe);
        }

        long startTime = System.currentTimeMillis();
        try (ProgressBarBuilder.ProgressBar progressBar = ProgressBarBuilder.Builder()
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
            this.relationshipGraph.syncStructuralTopLevelParentsToMarked();
            progressBar.setExtraMessage(String.format("Loaded %,d distinct Ids",
                                        this.relationshipGraph
                                            .getTotalDistinctChildCount()-currentIds));

            progressBar.refresh();
        } catch (IOException | CsvValidationException e) {
            logger.Print("IdChainSampler.importFile Error in reading CSV file " + file, e);
            throw new RuntimeException(e);
        }

        /*
            Only if it hasn't been previously set
         */
        this.actualDepth = relationshipGraph.getMaxDepthOverall();

        long latency = System.currentTimeMillis() - startTime;
        if(openTelemetry != null) {
            openTelemetry.setIdMgrGauge(file.getName(),
                                        labels,
                                        null,
                                        this.getIdCount(),
                                        this.getStartingIdsCount(),
                                        this.getDepth() + 1,
                                        this.getInitialDepth() + 1,
                                        this.getNbrRelationships(),
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

            try (ProgressBarBuilder.ProgressBar progressBar = ProgressBarBuilder.Builder()
                                                                .setInitialMax(this.relationshipGraph.getTotalDistinctChildCount())
                                                                .setTaskName("Exporting vertices ids")
                                                                .hideEta()
                                                                .build();
                 BufferedWriter writer = new BufferedWriter(new FileWriter(exportFile))) {

                progressBar.setExtraMessage(exportFile.getName());
                progressBar.refresh();

                final Set<Object> toplevelIds = this.relationshipGraph
                                                    .getTopLevelParents();

                toplevelIds.forEach(parentId -> {
                    try {
                        writer.write(parentId.toString());
                        writer.newLine();
                        progressBar.step();
                    } catch (IOException e) {
                        logger.Print("IdChainSampler.exportFile Error in exporting file " + filePath, e);
                    }
                });
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
    public void printStats(final LogSource logger) {
        final String msg = String.format("""
                                        Using Id Manager '%s':
                                          Number of Distinct Nodes: %,d
                                                    Starting Nodes: %,d
                                                    Required Depth: %,d
                                                     Relationships: %,d
                                                    Possible Depth: %,d""",
                                this.getClass().getSimpleName(),
                                this.getIdCount(),
                                this.getStartingIdsCount(),
                                this.getDepth() + 1,
                                this.getNbrRelationships(),
                                this.getInitialDepth() + 1);
        Helpers.Println(System.out,
                        msg,
                        Helpers.BLACK,
                        Helpers.GREEN_BACKGROUND);
        logger.info(msg);
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

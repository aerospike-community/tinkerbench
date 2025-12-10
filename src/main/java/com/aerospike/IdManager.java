package com.aerospike;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public interface IdManager {

    /*
    *   Determines if Ids actually exists (loaded/obtained)
    *   @return True to indicated Ids are present
     */
    boolean CheckIdsExists(final LogSource logger);

    /*
    *   Obtains Id from the Graph DB
     */
    void init(final GraphTraversalSource g,
                final OpenTelemetry openTelemetry,
                final LogSource logger,
                final int sampleSize,
                final String[] labels);

    /*
    *   Indicates the Id Manager has been Initialized and Ids may have been obtained/loaded.
     */
    boolean isInitialized();

    /*
    *   @return The total number of distinct ids
     */
    int getIdCount();
    /*
    *   @return Thte total number of Starting Ids (root/parents).
     */
    int getStartingIdsCount();

    boolean isEmpty();

    /*
    *   @return Obtains an random Id or depending on the manager, the Top-Level (parent) id.
     */
    Object getId();
    /*
    *   @return Returns a random Id at that depth in the tree. If a tree structure doesn't exist (has no depth)
    *               the argument is ignored and this behaves like getId()
     */
    Object getId(int depth);
    /*
    *   This resets the internal tree cache so the next getId(s) will return a new set of random ids, if required by the Id Manager.
    *       Ignored if the Id Manager doesn't support trees (depth)
     */
    void Reset();
    /*
    *   @return The request depth of a tree structure.
     */
    int getDepth();
    /*
     *   @return The loaded/obtained depth of a tree structure. A value of -1 indicates no Ids loaded.
     */
    int getInitialDepth();

    /*
    *   @return The number of relationships between ids
     */
    int getNbrRelationships();
    /*
    *   Set's the requested depth of a tree, if it exists. Otherwise, this is ignored.
    *   This should be set by a query to the actual depth required by the query to improve Id manager's performance.
    *   To obtain the depth when the Ids were actually loaded, see getInitialDepth.
     */
    void setDepth(int depth);

    /*
    *   @return This will return a collection of Ids based on the current tree where each array position represents the id at that depth.
    *               Example: [23, 72, 101, 45]
    *                       This tree has a maximum depth of 4 where index 0 (23) is the top-level parent,
    *                       Index 1 (72) -> Child of 23
    *                       Index 2 (101) -> Grandchild of 23
    *
    * Note: The tree can produce a new range set of id by calling Reset method.
     */
    Object[]  getIds();

    /*
    *   @param filePath A CSV file to be used to import Ids. This Path can contain wildcard chars or be a folder where al CSV files will be imported.
    *           The format of the CSV file is dependent on the Id Manager used.
     */
    long importFile(final String filePath,
                       final OpenTelemetry openTelemetry,
                       final LogSource logger,
                       final int sampleSize,
                       final String[] labels);

    void exportFile(final String filePath,
                    final LogSource logger);
}

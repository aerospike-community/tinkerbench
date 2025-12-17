package com.aerospike;

import java.util.*;

/**
 * A generic parent-child graph that:
 * - Supports multiple parents per node
 * - Supports cycles (circular references)
 * - Can query top-level parents and distinct children
 * - Can get direct children and all descendants up to a max depth
 * - Can compute maximum depth
 * - Can build relationships from a linear path like [1, 2, 4, 5]
 *
 * @param <T> the node type (must have stable equals/hashCode)
 */
public final class RelationshipGraph<T> {

    // parent -> children
    private final Map<T, Set<T>> children = new HashMap<>();

    // child -> parents
    private final Map<T, Set<T>> parents = new HashMap<>();

    // Explicitly marked roots
    private final Set<T> explicitTopLevelParents = new LinkedHashSet<>();

    private int distinctChildCount = 0;
    // cache of max depth from each node (simple path depth)
    private final Map<T, Integer> maxDepthCache = new HashMap<>();

    // cache: (node, remainingDepth) -> all paths starting at node
    private final Map<NodeDepthKey<T>, List<List<T>>> pathCache = new HashMap<>();
    private record NodeDepthKey<T>(T node, int remainingDepth) {}


    /**
     * Returns true if the graph contains no nodes and no relationships.
     */
    public boolean isEmpty() {
        return children.isEmpty() && parents.isEmpty();
        //return getAllNodes().isEmpty();
    }

    /**
     * Removes all nodes and all relationships from the graph.
     * After calling this, the graph will be empty.
     */
    public void clear() {
        children.clear();
        parents.clear();
        distinctChildCount = 0;
        maxDepthCache.clear();
        pathCache.clear();
        explicitTopLevelParents.clear();
    }

    public void markAsTopLevelParent(T node) {
        Objects.requireNonNull(node, "node must not be null");
        addNode(node); // ensure it exists in the graph
        explicitTopLevelParents.add(node);
    }

    public void unmarkAsTopLevelParent(T node) {
        explicitTopLevelParents.remove(node);
    }

    /**
     * Add a relationship parent -> child.
     * This does NOT forbid cycles.
     */
    public void addRelationship(T parent, T child) {
        Objects.requireNonNull(parent, "parent must not be null");
        Objects.requireNonNull(child, "child must not be null");

        boolean isNewChildRelationship =
                parents.getOrDefault(child, Set.of()).isEmpty();

        children.computeIfAbsent(parent, k -> new LinkedHashSet<>()).add(child);
        children.computeIfAbsent(child, k -> new LinkedHashSet<>());
        parents.computeIfAbsent(parent, k -> new LinkedHashSet<>());
        parents.computeIfAbsent(child, k -> new LinkedHashSet<>()).add(parent);

        if (isNewChildRelationship) {
            distinctChildCount++;
        }

        // graph structure changed -> invalidate depth cache
        maxDepthCache.clear();
        pathCache.clear();
    }


    /**
     * Ensure a node exists in the graph even if it has no relationships yet.
     */
    public void addNode(T node) {
        Objects.requireNonNull(node, "node must not be null");
        children.computeIfAbsent(node, k -> new LinkedHashSet<>());
        parents.computeIfAbsent(node, k -> new LinkedHashSet<>());
        maxDepthCache.clear();
        pathCache.clear();
    }


    /**
     * Add a chain of relationships from a list of nodes.
     * Example: [1, 2, 4, 5] adds:
     *  1 -> 2
     *  2 -> 4
     *  4 -> 5
     */
    public void addPath(List<T> nodes) {
        Objects.requireNonNull(nodes, "nodes must not be null");
        if (nodes.size() < 2) return;
        for (int i = 0; i < nodes.size() - 1; i++) {
            addRelationship(nodes.get(i), nodes.get(i + 1));
        }
        // addRelationship already clears the cache, so this is optional
    }

    /**
     * Varargs convenience for addPath.
     * Example: addPath(1, 2, 4, 5)
     * Note: If provided a single node, it will be treated as a marked parent
     */
    @SafeVarargs
    public final void addPath(T... nodes) {
        Objects.requireNonNull(nodes, "nodes must not be null");

        if (nodes.length == 0) return;
        if (nodes.length == 1){
            this.markAsTopLevelParent(nodes[0]);
            return;
        }
        for (int i = 0; i < nodes.length - 1; i++) {
            addRelationship(nodes[i], nodes[i + 1]); // addRelationship already clears the cache
        }

    }

    /**
     * Return all known nodes (parents and/or children).
     */
    public Set<T> getAllNodes() {
        Set<T> all = new LinkedHashSet<>(children.keySet());
        all.addAll(parents.keySet());
        return Collections.unmodifiableSet(all);
    }

    /**
     * Get the direct children of a parent.
     *
     * @param parent the parent node
     * @return unmodifiable set of direct children (empty if none or unknown parent)
     */
    public Set<T> getDirectChildren(T parent) {
        Objects.requireNonNull(parent, "parent must not be null");
        return Collections.unmodifiableSet(
                children.getOrDefault(parent, Set.of())
        );
    }

    /**
     * Get all descendants of a parent up to maxDepth.
     * maxDepth = 1 -> only direct children
     * maxDepth = 2 -> children and grandchildren
     *
     * Cycles are handled via a visited-set to avoid infinite loops.
     *
     * @param parent   starting node
     * @param maxDepth maximum depth (>= 1)
     * @return unmodifiable set of descendants (no ordering guarantee)
     */
    public Set<T> getDescendants(T parent, int maxDepth) {
        Objects.requireNonNull(parent, "parent must not be null");
        if (maxDepth < 1) {
            throw new IllegalArgumentException("maxDepth must be >= 1");
        }

        if (!children.containsKey(parent)) {
            return Set.of();
        }

        Set<T> result = new LinkedHashSet<>();
        Set<T> visited = new HashSet<>();
        Queue<NodeDepth<T>> queue = new ArrayDeque<>();

        visited.add(parent);
        queue.add(new NodeDepth<>(parent, 0));

        while (!queue.isEmpty()) {
            NodeDepth<T> nd = queue.poll();
            T current = nd.node();
            int depth = nd.depth();

            if (depth == maxDepth) {
                continue; // reached the limit
            }

            for (T child : children.getOrDefault(current, Set.of())) {
                if (!visited.contains(child)) {
                    visited.add(child);
                    result.add(child);
                    queue.add(new NodeDepth<>(child, depth + 1));
                }
            }
        }

        return Collections.unmodifiableSet(result);
    }

    /*
    * @return The total number of nodes
     */
    public int getTotal() {
        return this.getTotalDistinctChildCount() + this.getTopLevelParentCount();
    }
    /**
     * Get all top-level marked parents
     */
    public Set<T> getTopLevelParents() {
        return Collections.unmodifiableSet(explicitTopLevelParents);
    }

    /**
     * Find all top-level parents from the graph:
     * nodes that have NO parents (i.e., roots in this graph).
     */
    public Set<T> findTopLevelParents() {
        Set<T> all = getAllNodes();
        Set<T> roots = new LinkedHashSet<>();
        for (T node : all) {
            Set<T> ps = parents.getOrDefault(node, Set.of());
            if (ps.isEmpty()) {
                roots.add(node);
            }
        }
        return Collections.unmodifiableSet(roots);
    }

    /**
     * Ensures that all structural top-level parents (nodes with no parents)
     * are also recorded as explicit top-level parents.
     *
     * @return the set of nodes that were newly marked as top-level parents
     */
    public Set<T> syncStructuralTopLevelParentsToMarked() {
        Set<T> newlyMarked = new LinkedHashSet<>();

        // Structural roots: nodes with no parents
        for (T node : getAllNodes()) {
            if (parents.getOrDefault(node, Set.of()).isEmpty()) {
                // If this structural root was not yet explicitly marked,
                // add it and track it as newly marked.
                if (explicitTopLevelParents.add(node)) {
                    newlyMarked.add(node);
                }
            }
        }

        return Collections.unmodifiableSet(newlyMarked);
    }

    /**
     * Number of top-level parents.
     */
    public int getTopLevelParentCount() {
        return getTopLevelParents().size();
    }

    /**
     * Returns all descendant paths for each top-level parent.
     *
     * For each top-level parent (a node with no parents), this method returns
     * all root-to-descendant paths up to maxDepth.
     *
     * - Each path is a List<T> like [root, child, grandchild, ...].
     * - If a root has no children, it will still have a single path [root].
     * - Cycles are handled: nodes are not revisited in the same path.
     *
     * @param maxDepth maximum depth (number of edges) to traverse.
     *                 maxDepth = 1 => root -> child only
     *                 maxDepth = 2 => root -> child -> grandchild, etc.
     */
    public Map<T, List<List<T>>> getAllTopLevelPaths(int maxDepth) {
        if (maxDepth < 0) {
            throw new IllegalArgumentException("maxDepth must be >= 0");
        }

        Map<T, List<List<T>>> result = new LinkedHashMap<>();

        Set<T> roots = getTopLevelParents();
        if (roots.isEmpty()) {
            // fully cyclic graph: treat all nodes as potential roots
            roots = getAllNodes();
        }

        for (T root : roots) {
            Set<T> pathVisited = new HashSet<>();
            List<List<T>> pathsFromRoot = getPathsFrom(root, maxDepth, pathVisited);
            result.put(root, pathsFromRoot);
        }

        return result;
    }

    /**
     * Total number of DISTINCT children in the graph.
     * (nodes that have at least one parent).
     */
    public int getTotalDistinctChildCount() {
        return distinctChildCount;
    }

    /**
     * For convenience: number of parent->child relationships (edges).
     */
    public int getRelationshipCount() {
        int edges = 0;
        for (Set<T> ch : children.values()) {
            edges += ch.size();
        }
        return edges;
    }

    /**
     * Compute the maximum depth starting from a given root.
     * Depth = length of longest simple path:
     *  root alone      => 0
     *  root -> child   => 1
     *  root -> child -> grandchild => 2, etc.
     *
     * Uses memoization for performance and is cycle-safe.
     */
    public int getMaxDepthFrom(T root) {
        Objects.requireNonNull(root, "root must not be null");
        if (!children.containsKey(root)) {
            return 0;
        }
        return dfsMaxDepthCached(root, new HashSet<>());
    }

    /**
     * Compute the maximum depth over the entire graph,
     * starting from each top-level parent.
     *
     * If there are no top-level parents (pure cycles), falls back
     * to treating every node as a potential starting point.
     *
     * With memoization this is effectively O(V + E).
     */
    public int getMaxDepthOverall() {
        Set<T> roots = getTopLevelParents();
        if (roots.isEmpty()) {
            roots = getAllNodes(); // fallback if the graph is fully cyclic
        }

        int max = 0;
        // fresh pathVisited set per root
        for (T root : roots) {
            max = Math.max(max, getMaxDepthFrom(root));
        }
        return max;
    }

    /**
     * Depth-first traversal to collect all paths from a starting node.
     *
     * @param node         current node
     * @param depth        current depth (edges from root so far)
     * @param maxDepth     maximum depth allowed
     * @param pathVisited  nodes visited in the current path (for cycle detection)
     * @param currentPath  current path from root to this node
     * @param outPaths     collected paths
     */
    private void collectPathsFrom(T node,
                                    int depth,
                                    int maxDepth,
                                    Set<T> pathVisited,
                                    Deque<T> currentPath,
                                    List<List<T>> outPaths) {
        // Detect cycle in the current path; if seen, stop exploring this branch.
        if (!pathVisited.add(node)) {
            return;
        }

        currentPath.addLast(node);

        Set<T> kids = children.getOrDefault(node, Set.of());
        boolean isLeafByDepth = depth >= maxDepth;
        boolean isLeafByStructure = kids.isEmpty();

        if (isLeafByDepth || isLeafByStructure) {
            // We’ve reached either maxDepth or a structural leaf -> record the path.
            outPaths.add(List.copyOf(currentPath));
        } else {
            for (T child : kids) {
                collectPathsFrom(child, depth + 1, maxDepth, pathVisited, currentPath, outPaths);
            }
        }

        // backtrack
        currentPath.removeLast();
        pathVisited.remove(node);
    }

    /**
     * Returns all simple paths starting from 'node', with at most 'remainingDepth' edges.
     * Each path is a List<T> starting with 'node'.
     *
     * - Cycles are avoided via 'pathVisited'.
     * - Uses pathCache to avoid recomputing for the same (node, remainingDepth).
     */
    private List<List<T>> getPathsFrom(T node,
                                        int remainingDepth,
                                        Set<T> pathVisited) {
        if (remainingDepth < 0) {
            return List.of();
        }

        NodeDepthKey<T> key = new NodeDepthKey<>(node, remainingDepth);
        List<List<T>> cached = pathCache.get(key);
        if (cached != null) {
            return cached;
        }

        // cycle detection: if node already in current path, treat as leaf (no further expansion)
        if (!pathVisited.add(node)) {
            List<List<T>> selfPath = List.of(List.of(node));
            pathCache.put(key, selfPath);
            return selfPath;
        }

        Set<T> kids = children.getOrDefault(node, Set.of());

        // leaf by structure OR depth limit
        if (kids.isEmpty() || remainingDepth == 0) {
            List<List<T>> selfPath = List.of(List.of(node));
            pathCache.put(key, selfPath);
            pathVisited.remove(node);
            return selfPath;
        }

        List<List<T>> result = new ArrayList<>();

        for (T child : kids) {
            List<List<T>> childPaths = getPathsFrom(child, remainingDepth - 1, pathVisited);
            for (List<T> childPath : childPaths) {
                // prepend current node to each child path
                List<T> newPath = new ArrayList<>(childPath.size() + 1);
                newPath.add(node);
                newPath.addAll(childPath);
                result.add(newPath);
            }
        }

        pathVisited.remove(node);

        // store an immutable copy in the cache
        List<List<T>> immutableResult = List.copyOf(result);
        pathCache.put(key, immutableResult);
        return immutableResult;
    }

    // Depth-first search to compute max depth from 'node'
    private int dfsMaxDepth(T node, Set<T> pathVisited) {
        if (!pathVisited.add(node)) {
            // node already in current path -> cycle, stop here
            return 0;
        }

        int maxChildDepth = 0;
        for (T child : children.getOrDefault(node, Set.of())) {
            int d = dfsMaxDepth(child, pathVisited);
            if (d > maxChildDepth) {
                maxChildDepth = d;
            }
        }

        pathVisited.remove(node);
        // if no children, depth = 0; otherwise 1 + deepest child
        return children.getOrDefault(node, Set.of()).isEmpty()
                ? 0
                : 1 + maxChildDepth;
    }

    private int dfsMaxDepthCached(T node, Set<T> pathVisited) {
        // already computed?
        Integer cached = maxDepthCache.get(node);
        if (cached != null) {
            return cached;
        }

        // cycle detection: if this node is already in the current path,
        // treat as depth 0 to avoid infinite recursion
        if (!pathVisited.add(node)) {
            return 0;
        }

        int maxChildDepth = 0;
        Set<T> kids = children.getOrDefault(node, Set.of());
        for (T child : kids) {
            int d = dfsMaxDepthCached(child, pathVisited);
            if (d > maxChildDepth) {
                maxChildDepth = d;
            }
        }

        pathVisited.remove(node);

        int depth = kids.isEmpty() ? 0 : 1 + maxChildDepth;
        maxDepthCache.put(node, depth);
        return depth;
    }

    // helper record to track depth during BFS
    private record NodeDepth<T>(T node, int depth) {}

    /**
     * Print all nodes and their relationships in a readable tree-like form.
     * Cycle-safe: if a node is revisited in the same path, prints "(cycle)".
     */
    public void printGraph() {
        Set<T> roots = getTopLevelParents();
        if (roots.isEmpty()) {
            // Fully cyclic graph: no top-level parents
            System.out.println("(No top-level parents found — graph may be fully cyclic)");
            roots = getAllNodes();
        }

        for (T root : roots) {
            printSubGraph(root, 0, new HashSet<>());
            System.out.println();
        }
    }

    /**
     * Recursive helper for printing relationships.
     */
    private void printSubGraph(T node, int indent, Set<T> pathVisited) {
        printIndent(indent);

        if (pathVisited.contains(node)) {
            System.out.println(node + " (cycle)");
            return;
        }

        System.out.println(node);
        pathVisited.add(node);

        for (T child : children.getOrDefault(node, Set.of())) {
            printSubGraph(child, indent + 1, pathVisited);
        }

        pathVisited.remove(node);
    }

    private void printIndent(int indent) {
        final int pos = Math.max(0, indent);
        if(pos >= 1) {
            System.out.print(".".repeat(pos));
            System.out.print(" ");
        }
    }

    @Override
    public String toString() {
        return """
        RelationshipGraph {
            totalNodes           = %d
            totalEdges           = %d
            topLevelParents      = %d
            distinctChildCount   = %d
            maxDepthOverall      = %d
        }
        """.formatted(
                getAllNodes().size(),
                getRelationshipCount(),
                getTopLevelParentCount(),
                getTotalDistinctChildCount(),
                getMaxDepthOverall()
        );
    }
}


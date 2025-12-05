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

    private int distinctChildCount = 0;
    // cache of max depth from each node (simple path depth)
    private final Map<T, Integer> maxDepthCache = new HashMap<>();


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
    }


    /**
     * Ensure a node exists in the graph even if it has no relationships yet.
     */
    public void addNode(T node) {
        Objects.requireNonNull(node, "node must not be null");
        children.computeIfAbsent(node, k -> new LinkedHashSet<>());
        parents.computeIfAbsent(node, k -> new LinkedHashSet<>());
        maxDepthCache.clear();
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
     */
    @SafeVarargs
    public final void addPath(T... nodes) {
        Objects.requireNonNull(nodes, "nodes must not be null");
        if (nodes.length < 2) return;
        for (int i = 0; i < nodes.length - 1; i++) {
            addRelationship(nodes[i], nodes[i + 1]);
        }
        // addRelationship already clears the cache
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

    /**
     * Get all top-level parents:
     * nodes that have NO parents (i.e., roots in this graph).
     */
    public Set<T> getTopLevelParents() {
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
     * Number of top-level parents.
     */
    public int getTopLevelParentCount() {
        return getTopLevelParents().size();
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
            printSubGraph(child, indent + 2, pathVisited);
        }

        pathVisited.remove(node);
    }

    private void printIndent(int indent) {
        System.out.print(" ".repeat(Math.max(0, indent)));
    }
}


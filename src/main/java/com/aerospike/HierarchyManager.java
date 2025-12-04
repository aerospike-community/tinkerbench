package com.aerospike;

import java.util.*;

public class HierarchyManager {

    // Map to store parent-child relationships: parent -> set of children
    private final Map<Object, Set<Object>> parentToChildrenMap;

    public HierarchyManager() {
        this.parentToChildrenMap = new HashMap<>();
    }

    public boolean isEmpty() { return parentToChildrenMap.isEmpty(); }

    /**
     * Adds a path to the hierarchy structure
     * @param path Array representing parent-child relationships
     *
     * Example:
     *  //Sample data:
     *         int[][] paths = {
     *             {1, 364, 8}, //1-Parent, 364-Child, 8-Grandchild
     *             {1, 364, 11}, //1-Parent, 364-Child, 11-Grandchild
     *             {1, 364, 8, 182},
     *             {1, 364, 8, 44},
     *             {1, 47, 103},
     *             {1, 47, 237}
     *         };
     *         // Add all paths to the hierarchy
     *         for (int[] path : paths) {
     *             ids.addPath(path);
     *         }
     */
    public void addPath(Object[] path) {
        if (path == null || path.length == 0) {
            return;
        }

        // Process each parent-child relationship in the path
        for (int i = 0; i < path.length - 1; i++) {
            Object parent = path[i];
            Object child = path[i + 1];

            // Add child to parent's children set
            parentToChildrenMap.computeIfAbsent(parent, k -> new HashSet<>()).add(child);
        }
    }

    /**
     * Gets all direct children of a parent
     * @param parent The parent node
     * @return Set of direct children, empty set if none
     *
     * Examples:
     *      System.out.println("\n=== Direct Children of Node 1 ===");
     *      System.out.println(hierarchy.getDirectChildren(1));
     * === Direct Children of Node 1 ===
     * [364, 47]
     *
     *      System.out.println("\n=== Direct Children of Node 364 ===");
     *      System.out.println(hierarchy.getDirectChildren(364));
     * === Direct Children of Node 364 ===
     * [8, 11]
     */
    public List<Object> getDirectChildren(Object parent) {
        return new ArrayList<>(parentToChildrenMap.getOrDefault(parent, new HashSet<>()));
    }

    /**
     * Gets all descendants (children, grandchildren, etc.) of a parent
     * @param parent The parent node
     * @return Set of all descendants
     *
     * Examples:
     *      System.out.println("\n=== All Descendants of Node 1 ===");
     *      System.out.println(hierarchy.getAllDescendants(1));
     * === All Descendants of Node 1 ===
     * [182, 103, 8, 11, 364, 44, 237, 47]
     *
     *      System.out.println("\n=== All Descendants of Node 364 ===");
     *      System.out.println(hierarchy.getAllDescendants(364));
     * === All Descendants of Node 364 ===
     * [182, 8, 11, 44]
     */
    public List<Object> getAllDescendants(Object parent) {
        Set<Object> directChildren = parentToChildrenMap.getOrDefault(parent, new HashSet<>());

        // Add direct children
        List<Object> descendants = new ArrayList<>(directChildren);

        // Recursively add descendants of each child
        for (Object child : directChildren) {
            descendants.addAll(getAllDescendants(child));
        }

        return descendants;
    }

    /**
     * Gets descendants at a specific depth level
     * @param parent The parent node
     * @param depth The depth level (1 for direct children, 2 for grandchildren, etc.)
     * @return Set of descendants at the specified depth
     *
     * Example:
     *      System.out.println("\n=== Grandchildren of Node 1 (Depth 2) ===");
     *      System.out.println(hierarchy.getDescendantsAtDepth(1, 2));
     * === Grandchildren of Node 1 (Depth 2) ===
     * [103, 8, 11, 237]
     */
    public List<Object> getDescendantsAtDepth(Object parent, int depth) {
        if (depth <= 0) {
            return new ArrayList<>();
        }

        if (depth == 1) {
            return getDirectChildren(parent);
        }

        List<Object> result = new ArrayList<>();
        List<?> directChildren = getDirectChildren(parent);

        for (Object child : directChildren) {
            result.addAll(getDescendantsAtDepth(child, depth - 1));
        }

        return result;
    }

    /**
     * Checks if a node exists in the hierarchy
     * @param node The node to check
     * @return true if node exists, false otherwise
     */
    public boolean nodeExists(Object node) {
        return parentToChildrenMap.containsKey(node) ||
                parentToChildrenMap.values().stream().anyMatch(children -> children.contains(node));
    }

    /**
     * Gets all root nodes (nodes that have no children)
     * @return collection of root nodes
     *
     * Example:
     *      System.out.println("\n=== Root Nodes ===");
     *      System.out.println(hierarchy.getRootNodes());
     * === Root Nodes ===
     * [1]
     */
    public List<Object> getRootNodes() {
        List<Object> allNodes = new ArrayList<>();
        List<Object> allChildren = new ArrayList<>();

        // Collect all nodes and children
        for (Map.Entry<Object, Set<Object>> entry : parentToChildrenMap.entrySet()) {
            allNodes.add(entry.getKey());
            allChildren.addAll(entry.getValue());
            allNodes.addAll(entry.getValue());
        }

        // Root nodes are those that have no children
        List<Object> rootNodes = new ArrayList<>(allNodes);
        allChildren.forEach(rootNodes::remove);

        return rootNodes;
    }

    /**
     * Prints the hierarchy structure starting from root nodes
     * Example:
     * === Hierarchy Structure ===
     * 1
     *   364
     *     8
     *       182
     *       44
     *     11
     *   47
     *     103
     *     237
     */
    public void printHierarchy() {
        List<?> rootNodes = getRootNodes();
        for (Object root : rootNodes) {
            printNode(root, 0);
        }
    }

    private void printNode(Object node, int depth) {
        for (int i = 0; i < depth; i++) {
            System.out.print("  ");
        }
        System.out.println(node);

        List<?> children = getDirectChildren(node);
        for (Object child : children) {
            printNode(child, depth + 1);
        }
    }
}

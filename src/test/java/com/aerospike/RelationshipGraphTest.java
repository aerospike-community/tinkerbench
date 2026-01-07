package com.aerospike;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import java.util.*;

public class RelationshipGraphTest {

    private RelationshipGraph<String> graph;

    @BeforeEach
    void setUp() {
        graph = new RelationshipGraph<>();
    }

    @Test
    @DisplayName("Test empty graph")
    void testEmptyGraph() {
        assertTrue(graph.isEmpty());
        assertEquals(0, graph.getAllNodes().size());
        assertEquals(0, graph.getTopLevelParentCount());
        assertEquals(0, graph.getTotalDistinctChildCount());
        assertEquals(0, graph.getRelationshipCount());
        assertEquals(0, graph.getMaxDepthOverall());
    }

    @Test
    @DisplayName("Test add node")
    void testAddNode() {
        graph.addNode("A");
        assertFalse(graph.isEmpty());
        assertTrue(graph.getAllNodes().contains("A"));
        assertEquals(0, graph.getDirectChildren("A").size());
        assertEquals(0, graph.getTotalDistinctChildCount());
    }

    @Test
    @DisplayName("Test add relationship")
    void testAddRelationship() {
        graph.addRelationship("A", "B");

        assertTrue(graph.getAllNodes().contains("A"));
        assertTrue(graph.getAllNodes().contains("B"));
        assertTrue(graph.getDirectChildren("A").contains("B"));
        assertFalse(graph.getDirectChildren("B").contains("A")); // Not bidirectional
        assertEquals(1, graph.getTotalDistinctChildCount());
        assertEquals(1, graph.getRelationshipCount());
    }

    @Test
    @DisplayName("Test add path with list")
    void testAddPathWithList() {
        List<String> path = Arrays.asList("A", "B", "C", "D");
        graph.addPath(path);

        assertTrue(graph.getDirectChildren("A").contains("B"));
        assertTrue(graph.getDirectChildren("B").contains("C"));
        assertTrue(graph.getDirectChildren("C").contains("D"));
        assertEquals(3, graph.getRelationshipCount());
        assertEquals(3, graph.getTotalDistinctChildCount());
    }

    @Test
    @DisplayName("Test add path with varargs")
    void testAddPathWithVarargs() {
        graph.addPath("A", "B", "C", "D");

        assertTrue(graph.getDirectChildren("A").contains("B"));
        assertTrue(graph.getDirectChildren("B").contains("C"));
        assertTrue(graph.getDirectChildren("C").contains("D"));
        assertEquals(3, graph.getRelationshipCount());
    }

    @Test
    @DisplayName("Test single node path becomes top-level parent")
    void testSingleNodePath() {
        graph.addPath("A");

        assertTrue(graph.getTopLevelParents().contains("A"));
        assertEquals(0, graph.getRelationshipCount());
    }

    @Test
    @DisplayName("Test mark and unmark top-level parent")
    void testMarkUnmarkTopLevelParent() {
        graph.addNode("A");
        graph.markAsTopLevelParent("A");

        assertTrue(graph.getTopLevelParents().contains("A"));

        graph.unmarkAsTopLevelParent("A");
        assertFalse(graph.getTopLevelParents().contains("A"));
    }

    @Test
    @DisplayName("Test get direct children")
    void testGetDirectChildren() {
        graph.addRelationship("A", "B");
        graph.addRelationship("A", "C");
        graph.addRelationship("B", "D");

        Set<String> childrenOfA = graph.getDirectChildren("A");
        assertEquals(2, childrenOfA.size());
        assertTrue(childrenOfA.contains("B"));
        assertTrue(childrenOfA.contains("C"));

        Set<String> childrenOfB = graph.getDirectChildren("B");
        assertEquals(1, childrenOfB.size());
        assertTrue(childrenOfB.contains("D"));

        // Test for non-existent node
        Set<String> childrenOfX = graph.getDirectChildren("X");
        assertTrue(childrenOfX.isEmpty());
    }

    @Test
    @DisplayName("Test get descendants")
    void testGetDescendants() {
        graph.addPath("A", "B", "C", "D");
        graph.addPath("A", "E", "F");

        Set<String> descendantsDepth1 = graph.getDescendants("A", 1);
        assertEquals(2, descendantsDepth1.size());
        assertTrue(descendantsDepth1.contains("B"));
        assertTrue(descendantsDepth1.contains("E"));

        Set<String> descendantsDepth2 = graph.getDescendants("A", 2);
        assertEquals(4, descendantsDepth2.size());
        assertTrue(descendantsDepth2.contains("B"));
        assertTrue(descendantsDepth2.contains("C"));
        assertTrue(descendantsDepth2.contains("E"));
        assertTrue(descendantsDepth2.contains("F"));

        Set<String> descendantsDepth3 = graph.getDescendants("A", 3);
        assertEquals(5, descendantsDepth3.size());
        assertTrue(descendantsDepth3.contains("D"));
    }

    @Test
    @DisplayName("Test get descendants with invalid depth")
    void testGetDescendantsInvalidDepth() {
        graph.addRelationship("A", "B");

        assertThrows(IllegalArgumentException.class, () -> graph.getDescendants("A", 0));

        assertThrows(IllegalArgumentException.class, () -> graph.getDescendants("A", -1));
    }

    @Test
    @DisplayName("Test find top-level parents")
    void testFindTopLevelParents() {
        graph.addRelationship("A", "B");
        graph.addRelationship("C", "D");
        graph.addRelationship("E", "F");

        Set<String> roots = graph.findTopLevelParents();
        assertEquals(3, roots.size());
        assertTrue(roots.contains("A"));
        assertTrue(roots.contains("C"));
        assertTrue(roots.contains("E"));
    }

    @Test
    @DisplayName("Test sync structural top-level parents")
    void testSyncStructuralTopLevelParents() {
        graph.addRelationship("A", "B");
        graph.addRelationship("C", "D");

        Set<String> newlyMarked = graph.syncStructuralTopLevelParentsToMarked();
        assertEquals(2, newlyMarked.size());
        assertTrue(newlyMarked.contains("A"));
        assertTrue(newlyMarked.contains("C"));

        Set<String> topLevelParents = graph.getTopLevelParents();
        assertTrue(topLevelParents.contains("A"));
        assertTrue(topLevelParents.contains("C"));
    }

    @Test
    @DisplayName("Test get max depth from node")
    void testGetMaxDepthFrom() {
        graph.addPath("A", "B", "C", "D"); // depth 3
        graph.addPath("A", "E");           // depth 1

        assertEquals(3, graph.getMaxDepthFrom("A"));
        assertEquals(0, graph.getMaxDepthFrom("D")); // leaf node
    }

    @Test
    @DisplayName("Test get max depth overall")
    void testGetMaxDepthOverall() {
        graph.addPath("A", "B", "C", "D");
        graph.addPath("E", "F", "G");

        graph.markAsTopLevelParent("A");
        graph.markAsTopLevelParent("E");

        assertEquals(3, graph.getMaxDepthOverall());
    }

    @Test
    @DisplayName("Test cycle handling in max depth")
    void testCycleHandlingInMaxDepth() {
        graph.addRelationship("A", "B");
        graph.addRelationship("B", "C");
        graph.addRelationship("C", "A"); // Creates cycle

        graph.markAsTopLevelParent("A");

        // Should not infinite loop and should return finite value
        int depth = graph.getMaxDepthFrom("A");
        assertTrue(depth >= 0);
    }

    @Test
    @DisplayName("Test get all top-level paths")
    void testGetAllTopLevelPaths() {
        graph.addPath("A", "B", "C");
        graph.addPath("A", "D", "E");
        graph.markAsTopLevelParent("A");

        Map<String, List<List<String>>> paths = graph.getAllTopLevelPaths(2);
        assertTrue(paths.containsKey("A"));

        List<List<String>> pathsFromA = paths.get("A");
        assertEquals(2, pathsFromA.size());

        // Check that we have the expected paths
        boolean foundABC = false;
        boolean foundADE = false;

        for (List<String> path : pathsFromA) {
            if (path.equals(Arrays.asList("A", "B", "C"))) foundABC = true;
            if (path.equals(Arrays.asList("A", "D", "E"))) foundADE = true;
        }

        assertTrue(foundABC);
        assertTrue(foundADE);
    }

    @Test
    @DisplayName("Test multiple parents")
    void testMultipleParents() {
        graph.addRelationship("A", "C");
        graph.addRelationship("B", "C");
        graph.addRelationship("C", "D");

        graph.markAsTopLevelParent("A");
        graph.markAsTopLevelParent("B");

        Set<String> parentsOfC = graph.getParents("C");
        assertEquals(2, parentsOfC.size());
        assertTrue(parentsOfC.contains("A"));
        assertTrue(parentsOfC.contains("B"));

        assertEquals(3, graph.getRelationshipCount());
    }

    @Test
    @DisplayName("Test clear method")
    void testClear() {
        graph.addRelationship("A", "B");
        graph.addRelationship("B", "C");
        graph.markAsTopLevelParent("A");

        assertFalse(graph.isEmpty());
        assertEquals(3, graph.getAllNodes().size());

        graph.clear();

        assertTrue(graph.isEmpty());
        assertEquals(0, graph.getAllNodes().size());
        assertEquals(0, graph.getTopLevelParentCount());
        assertEquals(0, graph.getTotalDistinctChildCount());
    }

    @Test
    @DisplayName("Test get nodes at depth from bottom")
    void testGetNodesAtDepthFromBottom() {
        graph.addPath("Root", "A", "B", "C", "Leaf");
        graph.addPath("Root", "D", "E", "Leaf");

        graph.markAsTopLevelParent("Root");

        // Test bottom nodes (depth 0 from bottom)
        Set<String> bottomNodes = graph.getNodesAtDepthFromBottom("Root", 4, 0);
        assertEquals(1, bottomNodes.size());
        assertTrue(bottomNodes.contains("Leaf"));

        // Test one level up
        Set<String> oneUp = graph.getNodesAtDepthFromBottom("Root", 4, 1);
        assertEquals(2, oneUp.size());
        assertTrue(oneUp.contains("C"));
        assertTrue(oneUp.contains("E"));

        // out of range depth
        Set<String> depth60 = graph.getNodesAtDepthFromBottom("Root", 6, 0);
        assertEquals(1, depth60.size());
        assertTrue(depth60.contains("Leaf"));

        // out of range depth
        Set<String> depth61 = graph.getNodesAtDepthFromBottom("Root", 6, 1);
        assertEquals(2, depth61.size());
        assertTrue(depth61.contains("C"));
        assertTrue(depth61.contains("E"));

    }

    @Test
    @DisplayName("Test get nodes up to depth from bottom")
    void testGetNodesUpToDepthFromBottom() {
        graph.addPath("Root", "A", "B", "C");
        graph.markAsTopLevelParent("Root");

        Set<String> upToZero = graph.getNodesUpToDepthFromBottom("Root", 3, 0);
        assertEquals(1, upToZero.size());
        assertTrue(upToZero.contains("C")); // bottom node

        Set<String> upToOne = graph.getNodesUpToDepthFromBottom("Root", 3, 1);
        assertEquals(2, upToOne.size());
        assertTrue(upToOne.contains("C"));
        assertTrue(upToOne.contains("B"));
    }

    @Test
    @DisplayName("Test null parameter handling")
    void testNullParameterHandling() {
        assertThrows(NullPointerException.class, () -> graph.addRelationship(null, "B"));

        assertThrows(NullPointerException.class, () -> graph.addRelationship("A", null));

        assertThrows(NullPointerException.class, () -> graph.markAsTopLevelParent(null));

        assertThrows(NullPointerException.class, () -> graph.getDirectChildren(null));

        assertThrows(NullPointerException.class, () -> graph.getMaxDepthFrom(null));
    }

    @Test
    @DisplayName("Test immutability of returned collections")
    void testImmutability() {
        graph.addRelationship("A", "B");
        graph.markAsTopLevelParent("A");

        Set<String> children = graph.getDirectChildren("A");
        assertThrows(UnsupportedOperationException.class, () -> children.add("C"));

        Set<String> topLevel = graph.getTopLevelParents();
        assertThrows(UnsupportedOperationException.class, () -> topLevel.add("B"));
    }

    @Test
    @DisplayName("Test complex cycle scenario")
    void testComplexCycle() {
        // Create a more complex cycle: A -> B -> C -> D -> B
        graph.addRelationship("A", "B");
        graph.addRelationship("B", "C");
        graph.addRelationship("C", "D");
        graph.addRelationship("D", "B");

        graph.markAsTopLevelParent("A");

        // Should handle without infinite loop
        Set<String> descendants = graph.getDescendants("A", 10);
        assertFalse(descendants.isEmpty());

        int maxDepth = graph.getMaxDepthFrom("A");
        assertTrue(maxDepth >= 0); // Should be finite
    }

    @Test
    @DisplayName("Test toString method")
    void testToString() {
        graph.addPath("A", "B", "C");
        graph.markAsTopLevelParent("A");

        String str = graph.toString();
        assertNotNull(str);
        assertFalse(str.isEmpty());
        assertTrue(str.contains("totalNodes"));
        assertTrue(str.contains("totalEdges"));
    }

    @Test
    @DisplayName("Test getAllNodes method")
    void testGetAllNodes() {
        graph.addRelationship("A", "B");
        graph.addRelationship("B", "C");
        graph.addNode("D"); // isolated node

        Set<String> allNodes = graph.getAllNodes();
        assertEquals(4, allNodes.size());
        assertTrue(allNodes.contains("A"));
        assertTrue(allNodes.contains("B"));
        assertTrue(allNodes.contains("C"));
        assertTrue(allNodes.contains("D"));
    }

    @Test
    @DisplayName("Test get total method")
    void testGetTotal() {
        graph.addRelationship("A", "B");
        graph.addRelationship("A", "C");
        graph.addNode("D"); // isolated node
        graph.markAsTopLevelParent("A");
        graph.markAsTopLevelParent("D");

        int total = graph.getTotal();
        assertEquals(4, total); // A, B, C, D
    }
}

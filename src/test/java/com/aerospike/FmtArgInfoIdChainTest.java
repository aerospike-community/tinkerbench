package com.aerospike;

import com.aerospike.idmanager.IdChainSampler;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.Arguments;

import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;

//@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class FmtArgInfoIdChainTest<T> {

    protected final IdChainSampler idManager;

    //@BeforeEach
    protected FmtArgInfoIdChainTest(final T[][] relationships) {
        final LogSource logSource = LogSource.getInstance();

        printChildClassName();

        idManager = new IdChainSampler();
        idManager.addPath(relationships);
        assertTrue(idManager.CheckIdsExists(logSource), "There should be elements");

        assertArrayEquals(relationships,
                            new Object[][] {  idManager.getIds() }, "Individual Id Check");

        //idManager.printStats(logSource);
    }

    @ParameterizedTest
    @MethodSource("provideGremlinStringsTesting")
    public void testGremlinStrFmts(String gString,
                                    String expectedGString,
                                    String expectedQuery,
                                    int nbrArgs,
                                    int maxPos,
                                    boolean hasDepthUpArgs,
                                    Object[] expectedIds) {
        final FmtArgInfo fmtInfo = new FmtArgInfo(gString, idManager);
        String fmtStmt = fmtInfo.init();

        assertEquals(hasDepthUpArgs, fmtInfo.hasDepthUpArgs(), "Depth Up Check");
        assertEquals(expectedGString, fmtInfo.gremlinString, String.format("Check Format Strings: '%s'", gString));
        assertEquals(fmtInfo.args.length, nbrArgs, String.format("Number of Args Check: '%s'", gString));
        assertEquals(fmtInfo.length(), nbrArgs, String.format("Check Length Check: '%s'", gString));
        assertEquals(fmtInfo.maxArgsPosition, maxPos,  String.format("Check Max Position Check: '%s'", gString));
        assertArrayEquals(expectedIds, fmtInfo.getIds(), gString);

        assertEquals(expectedQuery, String.format(fmtStmt, fmtInfo.getIds()), gString);
    }

    protected void printChildClassName() {
        // Get the fully qualified name (e.g., com.example.tests.ChildATest)
        String fullName = this.getClass().getName();

        Helpers.Println(System.out,
                    "Testing Child Class Name: " + fullName,
                    Helpers.BLACK,
                    Helpers.GREEN_BACKGROUND);
    }

    public abstract Stream<Arguments> provideGremlinStringsTesting();
}
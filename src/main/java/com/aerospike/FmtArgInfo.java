package com.aerospike;

import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FmtArgInfo {
    ///This is a much more complete regex to parse the Gremlin string. This will allow an advance Id Manager based on String format params...
    final static Pattern fmtargPattern = Pattern.compile("(?<arg>(?<begin>['\"][^%]*)?%(?<opts>(?:(?<pos>\\d+)\\$)?(?:[-#+ 0,(<]*)?(?:\\d*)?(?:\\.\\d*)?(?:[tT])?)(?:[a-zA-Z])(?<end>[^)'\"]*['\"])?)");

    public String fmtArgValue;
    public final int depth;
    public char beginQuote;
    public char endQuote;

    public  FmtArgInfo(Matcher fmtargMatch) {
        this.fmtArgValue = fmtargMatch.group("arg");
        String grpValue = fmtargMatch.group("pos");
        if (grpValue != null) {
            this.depth = Integer.parseInt(grpValue);
        }
        else {
            this.depth = -1;
        }
        grpValue = fmtargMatch.group("begin");
        if (grpValue != null) {
            this.beginQuote = grpValue.charAt(0);
        } else {
            this.beginQuote = '\0';
        }
        grpValue = fmtargMatch.group("end");
        if (grpValue != null) {
            this.endQuote = grpValue.charAt(0);
        }  else {
            this.endQuote = '\0';
        }
    }

    public boolean hasQuotes() { return  this.beginQuote != '\0' && this.beginQuote == this.endQuote; }

    /*
     *   @params gremlinString -- The Gremlin String that will be seached looking for Format Arguments.
     *   @return A pair of Format Arguments Info and the Maximum Depth.
     *           If depth is -1, no format arguments found...
     */
    public static Pair<FmtArgInfo[], Integer> determineFmtArgs(final String gremlinString) {
        final Matcher fmtargMatch = fmtargPattern.matcher(gremlinString);
        final List<FmtArgInfo> fmtArgs = new ArrayList<>();
        int maxDepth = -1;

        while (fmtargMatch.find()) {
            final FmtArgInfo fmtArg = new FmtArgInfo(fmtargMatch);
            fmtArgs.add(fmtArg);
            if(maxDepth < fmtArg.depth) {
                maxDepth = fmtArg.depth;
            }
        }
        return Pair.with(fmtArgs.toArray(new FmtArgInfo[0]), maxDepth);
    }

    /*
     *   This will ensure proper quoting in the Gremlin String based on the Id data type...
     */
    public static String determineGremlinString(final Pair<FmtArgInfo[], Integer> fmtArgsDepth,
                                                final IdManager idManager,
                                                final String gremlinString) {

        if(fmtArgsDepth == null || idManager == null || gremlinString == null) {
            return gremlinString;
        }

        final Object sampleId = idManager.getId();
        final boolean isString = sampleId instanceof String;

        if(!isString) { return gremlinString; }

        int pos = 0;
        String newGremlinString = gremlinString;

        for (FmtArgInfo fmtArg : fmtArgsDepth.getValue0()) {
            if(!fmtArg.hasQuotes()) {
                String replaceArg = "";
                if(fmtArg.beginQuote == '\0') {
                    replaceArg = "\"";
                }
                replaceArg += fmtArg.fmtArgValue;
                if(fmtArg.endQuote == '\0') {
                    replaceArg += "\"";
                }
                newGremlinString = Helpers.ReplaceNthOccurrence(newGremlinString,
                        fmtArg.fmtArgValue,
                        replaceArg,
                        pos);
            }
            pos += 1;
        }
        return newGremlinString;
    }

    public static Object[] getIds(final Pair<FmtArgInfo[], Integer> fmtArgsDepth,
                                  final IdManager idManager) {

        final Object[] depthIds = idManager.getIds();

        if (fmtArgsDepth.getValue0().length == 0) {
            return depthIds;
        }

        int idPos = 0;
        List<Object> ids = new ArrayList<>();

        for (FmtArgInfo fmtArg : fmtArgsDepth.getValue0()) {
            if (fmtArg.depth < 0) {
                ids.add(depthIds[0]);
            } else if (fmtArg.depth > depthIds.length) {
                ids.add(null);
            } else {
                ids.add(depthIds[fmtArg.depth - 1]);
            }
        }

        return ids.toArray();
    }
}

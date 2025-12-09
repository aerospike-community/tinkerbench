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
    public final int position;
    public final boolean notPostional;
    public char beginQuote;
    public char endQuote;

    public  FmtArgInfo(Matcher fmtargMatch) {
        this.fmtArgValue = fmtargMatch.group("arg");
        String grpValue = fmtargMatch.group("pos");
        if (grpValue != null) {
            this.position = Integer.parseInt(grpValue);
            this.notPostional = false;
        }
        else {
            this.position = 1;
            this.notPostional = true;
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
     *   @params gremlinString -- The Gremlin String that will be searched looking for Format Arguments.
     *   @return A pair of Format Arguments Info and the Maximum Position within the format string.
     *           If position is -1, no format arguments found...
     */
    public static Pair<FmtArgInfo[], Integer> determineFmtArgs(final String gremlinString) {
        final Matcher fmtargMatch = fmtargPattern.matcher(gremlinString);
        final List<FmtArgInfo> fmtArgs = new ArrayList<>();
        int maxPos = -1;
        boolean allNonPostional = true;

        while (fmtargMatch.find()) {
            final FmtArgInfo fmtArg = new FmtArgInfo(fmtargMatch);
            fmtArgs.add(fmtArg);
            if(maxPos < fmtArg.position) {
                maxPos = fmtArg.position;
            }
            if(!fmtArg.notPostional) {
                allNonPostional = false;
            }
        }
        if(allNonPostional && fmtArgs.size() > maxPos) {
            maxPos = fmtArgs.size();
        }
        return Pair.with(fmtArgs.toArray(new FmtArgInfo[0]), maxPos);
    }

    /*
     *   This will ensure proper quoting in the Gremlin String based on the Id data type...
     */
    public static String determineGremlinString(final Pair<FmtArgInfo[], Integer> fmtArgsPosition,
                                                final IdManager idManager,
                                                final String gremlinString) {

        if(fmtArgsPosition == null || idManager == null || gremlinString == null) {
            return gremlinString;
        }

        final Object sampleId = idManager.getId();
        final boolean isString = sampleId instanceof String;

        if(!isString) { return gremlinString; }

        int pos = 0;
        String newGremlinString = gremlinString;

        for (FmtArgInfo fmtArg : fmtArgsPosition.getValue0()) {
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

    public static Object[] getIds(final Pair<FmtArgInfo[], Integer> fmtArgsPosition,
                                  final IdManager idManager) {

        idManager.Reset();
        return idManager.getIds();
    }
}

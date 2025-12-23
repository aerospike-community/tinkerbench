package com.aerospike;

import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FmtArgInfo {
    ///This is a much more complete regex to parse the Gremlin string. This will allow an advance Id Manager based on String format params...
    final static Pattern fmtargPattern = Pattern.compile("(?<arg>(?<begin>['\"][^%]*)?%(?<opts>(?:(?<pos>\\d+)\\$)?(?:[-#+ 0,(<]*)?(?:\\d*)?(?:\\.\\d*)?(?:[tT])?)(?:[a-zA-Z])(?<end>[^)'\"]*['\"])?)");

    final int positions;
    final FmtArg[] args;
    String gremlinString;
    final IdManager idManager;

    public static final class FmtArg {

        public String fmtArgValue;
        public final int position;
        public final boolean notPostional;
        public char beginQuote;
        public char endQuote;

        public FmtArg(Matcher fmtargMatch) {
            this.fmtArgValue = fmtargMatch.group("arg");
            String grpValue = fmtargMatch.group("pos");
            if (grpValue != null) {
                this.position = Integer.parseInt(grpValue);
                this.notPostional = false;
            } else {
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
            } else {
                this.endQuote = '\0';
            }
        }

        public boolean hasQuotes() {
            return this.beginQuote != '\0' && this.beginQuote == this.endQuote;
        }
    }

    /*
     *   @params gremlinString -- The Gremlin String that will be searched looking for Format Arguments.
     */
    public FmtArgInfo(final String gremlinString,
                      IdManager idManager) {

        this.gremlinString = gremlinString;
        this.idManager = idManager;

        final Matcher fmtargMatch = fmtargPattern.matcher(this.gremlinString);
        final List<FmtArg> fmtArgs = new ArrayList<>();
        int maxPos = -1;
        boolean allNonPostional = true;

        while (fmtargMatch.find()) {
            final FmtArg fmtArg = new FmtArg(fmtargMatch);
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

        this.positions = maxPos;
        this.args = fmtArgs.toArray(new FmtArg[0]);

        this.idManager.setDepth(this.positions - 1);
    }

    /*
     *   This will ensure proper quoting in the Gremlin String based on the Id data type...
     *   @return a new properly formated gremlin string.
     */
    public String determineGremlinString() {

        final Object sampleId = idManager.getId();
        final boolean isString = sampleId instanceof String;

        if(!isString) { return gremlinString; }

        int pos = 0;
        String newGremlinString = gremlinString;

        for (FmtArg fmtArg : this.args) {
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

        this.gremlinString = newGremlinString;
        return this.gremlinString;
    }

    public Object[] getIds() {

        idManager.Reset();
        return idManager.getIds();
    }

    public Object[] getIds(int noNullsTries) {

        Object[] ids = getIds();

        if(noNullsTries == 0) { return ids; }

        if(Arrays.stream(ids).anyMatch(Objects::isNull)) {
            return getIds(noNullsTries-1);
        }
        return ids;
    }

    public int length() { return this.args.length; }
    public int maxPositions() { return this.positions; }
}

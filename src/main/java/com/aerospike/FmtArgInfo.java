package com.aerospike;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FmtArgInfo {
    ///This is a much more complete regex to parse the Gremlin string. This will allow an advance Id Manager based on String format params...
    final static Pattern fmtargPattern = Pattern.compile("(?<arg>(?<begin>['\"][^%]*)?%(?<opts>(?:(?<pos>\\d+)\\$)?(?:[-#+ 0,(<]*)?(?:\\d*)?(?:\\.\\d*)?(?:[tT])?)(?:[a-zA-Z])(?<end>[^)'\"]*['\"])?)");

    final int maxArgs;
    final FmtArg[] args;
    final IdManager idManager;
    String gremlinString;

    public static final class FmtArg {

        public String fmtArgValue;
        public final int position;
        /// If true, this argument was explicitly reference as a depth (position) within a graph
        public final boolean Positional;
        public char beginQuote;
        public char endQuote;

        public FmtArg(Matcher fmtargMatch) {
            this.fmtArgValue = fmtargMatch.group("arg");
            String grpValue = fmtargMatch.group("pos");
            if (grpValue != null) {
                this.position = Integer.parseInt(grpValue);
                this.Positional = true;
            } else {
                this.position = 1;
                this.Positional = false;
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

        {
            //Convert any non-positional fmt placeholders (e.g., '%s') to a positional argument.
            final String regex = "%([a-zA-Z])";
            // The replacement string uses '$1' to refer to the captured letter (group 1)
            final String replacement = "%1\\$$1"; // The '$' needs to be escaped in the replacement string

            this.gremlinString = gremlinString.replaceAll(regex, replacement);
        }

        this.idManager = idManager;

        final Matcher fmtargMatch = fmtargPattern.matcher(this.gremlinString);
        final List<FmtArg> fmtArgs = new ArrayList<>();
        int maxPos = -1;

        while (fmtargMatch.find()) {
            final FmtArg fmtArg = new FmtArg(fmtargMatch);
            fmtArgs.add(fmtArg);
            if(maxPos < fmtArg.position) {
                maxPos = fmtArg.position;
            }
        }

        this.maxArgs = maxPos <=0 ? 1 : maxPos;
        this.args = fmtArgs.toArray(new FmtArg[0]);

        this.idManager.setDepth(this.maxArgs - 1);
    }

    /*
     *   This will ensure proper quoting in the Gremlin String based on the Id data type...
     *
     *   @return a newly properly formated gremlin string with proper quoting...
     *
     * Typical Example:
     *   String.format(fmtObj.determineGremlinString, fmtObj.getIds());
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

    /*
    *   @return Array of Ids from the Id Manager such that they can be used with the Gremlin Format String
    *
    * Typical Example:
    *   String.format(fmtObj.determineGremlinString(), fmtObj.getIds());
    */
    public Object[] getIds() {
        idManager.Reset();
        return idManager.getIds();
    }

    /*
    *   @parms noNullsTries -- The number of tries to obtain no null values
    *   @return Array of Ids from the Id Manager that contains no null values
    */
    public Object[] getIds(int noNullsTries) {

        Object[] ids = getIds();

        if(noNullsTries == 0) { return ids; }

        if(Arrays.stream(ids).anyMatch(Objects::isNull)) {
            return getIds(noNullsTries-1);
        }
        return ids;
    }

    /*
        @return The number of format placeholders used within the Gremlin string
     */
    public int length() { return this.args.length; }

    /*
        @return The maximum detected placeholder position found in the Gremlin string.
                This represents the required depth (under root node) to stratify the Gremlin Query.
     */
    public int maxArgs() { return this.maxArgs; }
}

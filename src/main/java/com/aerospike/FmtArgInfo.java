package com.aerospike;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class FmtArgInfo {
    ///This is a much more complete regex to parse the Gremlin string. This will allow an advance Id Manager based on String format params...
    final static Pattern fmtargPattern = Pattern.compile("(?<arg>(?<begin>['\"][^%,]*)?%(?<opts>(?:(?<pos>\\-?\\d+)\\$)?(?:[-#+ 0,(<]*)?(?:\\d*)?(?:\\.\\d*)?(?:[tT])?)(?<type>[a-zA-Z])[^),'\"]*(?<end>['\"])?)");

    final FmtArg[] args;
    final IdManager idManager;
    /// If true, one of the format argument instances uses a negative position (reference Id from bottom child up this number of levels)
    final boolean hasDepthUpArgs;

    String fmtArgString;
    String gremlinString;
    int maxArgsPosition;

    public static final class FmtArg {

        public final String fmtType;
        public final String fmtArgValue;

        /// The argument original position defined in the gremlin string
        /// '%-4$s' -> -4
        /// '%4$s' -> 4
        public final int argPosition;
        /// If true, this argument was explicitly reference as a depth (position) within a graph
        public final boolean Positional;
        /// The actual position within the id array.
        /// If < 0, indicates that this is a negative arg position.
        ///     Note: This will be changed when the init method is run. In this case it represents the position in the id array.
        /// '%-4$s' -> -1 (changed when 'init' method is executed)
        /// '%4$s' -> 4
        public int position;
        public char beginQuote;
        public char endQuote;
        public String phVarName;

        public FmtArg(Matcher fmtargMatch) {
            this.fmtArgValue = fmtargMatch.group("arg");
            this.fmtType = fmtargMatch.group("type");
            String grpValue = fmtargMatch.group("pos");
            if (grpValue != null) {
                this.argPosition = Integer.parseInt(grpValue);
                this.Positional = true;
            } else {
                this.argPosition = 1;
                this.Positional = false;
            }
            if(this.argPosition >= 0) {
                this.position = this.argPosition;
            } else {
                this.position = -1;
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

        @Override
        public String toString() {
            return String.format("FmtArg{'arg':'%s', 'argpos':%d, 'vlupos':%d, 'ispos':%s, 'quotes':%s}",
                                    this.fmtArgValue,
                                    this.argPosition,
                                    this.position,
                                    this.Positional,
                                    this.hasQuotes());
        }
    }

    /*
     *   @params gremlinString -- The Gremlin String that will be searched looking for Format Arguments.
     *
     * Note: The ids within the Id Manager may NOT be populated when this class is instantiated.
     */
    public FmtArgInfo(final String gremlinString,
                      IdManager idManager) {

        {
            //Convert any non-positional fmt placeholders (e.g., '%s') to a positional argument.
            final String regex = "%([a-zA-Z])";
            // The replacement string uses '$1' to refer to the captured letter (group 1)
            final String replacement = "%1\\$$1"; // The '$' needs to be escaped in the replacement string

            this.gremlinString = gremlinString.replaceAll(regex, replacement);
            this.fmtArgString = this.gremlinString;
        }

        this.idManager = idManager;

        final Matcher fmtargMatch = fmtargPattern.matcher(this.gremlinString);
        final List<FmtArg> fmtArgs = new ArrayList<>();
        int maxPos = -1;
        boolean depthUp = false;

        while (fmtargMatch.find()) {
            final FmtArg fmtArg = new FmtArg(fmtargMatch);
            fmtArgs.add(fmtArg);

            if(fmtArg.position < 0) {
                depthUp = true;
            } else if(maxPos < fmtArg.position) {
                maxPos = fmtArg.position;
            }
        }

        this.hasDepthUpArgs = depthUp;
        this.maxArgsPosition = maxPos <=0 ? 1 : maxPos;
        this.args = fmtArgs.toArray(new FmtArg[0]);

        if(this.hasDepthUpArgs) {
            this.idManager.setDepth(-1);
        } else {
            this.idManager.setDepth(this.maxArgsPosition - 1);
        }
    }

    /*
     *   This will ensure proper initialization of this instance...
     *   This must be called before any Ids are requested or use of the Gremlin Query String.
     *
     *   @return a newly properly formated gremlin string to be used by String.format...
     *
     * Typical Example:
     *   String.format(fmtObj.determineGremlinString(), fmtObj.getIds());
     */
    public String init() {

        if(this.hasDepthUpArgs) {
            // Fix up the gremlin format string to the proper position within the id array
            final int maxDepth = this.idManager.getDepth() + 1;
            for(FmtArg fmtArg : this.args) {
                if(fmtArg.position < 0) {
                    final int argPos = fmtArg.argPosition * -1;
                    int newPos = 1;
                    if(argPos < maxDepth) {
                        newPos = maxDepth - argPos + 1;
                    }
                    this.fmtArgString = this.fmtArgString.replaceAll(String.format("\\%%%d\\$", fmtArg.argPosition),
                                                                        String.format("\\%%%d\\$", newPos));
                    fmtArg.position = newPos;
                    if(newPos > this.maxArgsPosition) {
                        this.maxArgsPosition =  newPos;
                    }
                }
            }
        }

        this.determineGremlinString();
        return this.fmtArgString;
    }

    /*
     *   This will ensure proper quoting in the Gremlin String based on the Id data type...
     *
     *   @return a newly properly formated gremlin string with proper quoting...
     *
     * Typical Example:
     *   String.format(fmtObj.determineGremlinString, fmtObj.getIds());
     */
    private void determineGremlinString() {

        final Object sampleId = idManager.getId();
        final boolean isString = sampleId instanceof String;

        String newGremlinString = gremlinString;

        if(!isString) {
            for (FmtArg fmtArg : this.args) {
                fmtArg.phVarName = "phTBVar" + fmtArg.position;
                newGremlinString = Helpers.ReplaceNthOccurrence(newGremlinString,
                                                                fmtArg.fmtArgValue,
                                                                fmtArg.phVarName,
                                                                1);
            }
            this.gremlinString = newGremlinString;
            return;
        }

        int pos = 1;
        String newFmtString = fmtArgString;

        final Map<String,Integer> argPos = new HashMap<>();

        for (FmtArg fmtArg : this.args) {
            final String fmtArgValue = fmtArg.argPosition < 0 ? String.format("%%%d$%s", fmtArg.position, fmtArg.fmtType) : fmtArg.fmtArgValue;

            pos = argPos.merge(String.format("%d.%s", fmtArg.position, fmtArg.fmtType),
                                1,
                                Integer::sum);

            if(!fmtArg.hasQuotes()) {
                String replaceArg = "";
                if(fmtArg.beginQuote == '\0') {
                    replaceArg = "\"";
                }
                replaceArg += fmtArgValue;
                if(fmtArg.endQuote == '\0') {
                    replaceArg += "\"";
                }
                newFmtString = Helpers.ReplaceNthOccurrence(newFmtString,
                                                                fmtArgValue,
                                                                replaceArg,
                                                                pos);
            }

            fmtArg.phVarName = "phTBVar" + fmtArg.position;
            newGremlinString = Helpers.ReplaceNthOccurrence(newGremlinString,
                                                            fmtArg.fmtArgValue,
                                                            fmtArg.phVarName,
                                                            1);
        }

        this.fmtArgString = newFmtString;
        this.gremlinString = newGremlinString;
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
    public int maxArgs() { return this.maxArgsPosition; }

    public boolean hasDepthUpArgs() { return this.hasDepthUpArgs; }

    public FmtArg[] args() { return this.args; }

    public String gremlinString() { return this.gremlinString; }
    public String fmtArgString() { return this.fmtArgString; }
}

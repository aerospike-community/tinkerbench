package com.aerospike;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GraphConfigOptions {

    private static final Pattern kvPattern = Pattern.compile("^(?<key>[^=]+)=(?<value>.+)$");

    private final String key;
    private final Object value;

    public static GraphConfigOptions Create(String kvpStr) {
        if(kvpStr == null || kvpStr.isEmpty()) {
            throw new IllegalArgumentException("Graph Option's argument cannot be null or an empty string");
        }

        Matcher m = kvPattern.matcher(kvpStr.trim());
        if(!m.matches()) {
            throw new IllegalArgumentException(String.format("Graph Option's argument '%s' is not a valid key", kvpStr));
        }

        return new GraphConfigOptions(m.group("key"),
                                        Helpers.DetermineValue(m.group("value")));
    }

    public GraphConfigOptions(String key, Object value) {
        this.key = key;
        this.value = value;
    }

    public String getKey() { return key; }
    public Object getValue() { return value; }

    @Override
    public String toString() { return key + "=" + value; }
}

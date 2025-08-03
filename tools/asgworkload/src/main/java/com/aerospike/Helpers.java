package com.aerospike;

import org.javatuples.Pair;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Helpers {

    public static final double NS_TO_MS = 1000000D;
    public static final double NS_TO_US = 1000D;

    public static QueryRunnable GetQuery(String queryName,
                                         WorkloadProvider provider,
                                         AGSGraphTraversal graphTraversal,
                                         IdManager idManager,
                                         boolean debug) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final LogSource logger = LogSource.getInstance();

        try {

            logger.PrintDebug("GetQuery", "Creating Instance %s", queryName);

            Class<?> queryClass = Class.forName("com.aerospike." + queryName);
            Constructor<?> constructor = queryClass.getConstructor(WorkloadProvider.class, AGSGraphTraversal.class, IdManager.class);
            logger.PrintDebug("GetQuery", "Created  Class %s", queryClass.getName());

            // Instantiate the class by providing arguments to the constructor
            Object instance = constructor.newInstance(provider, graphTraversal, idManager);

            logger.PrintDebug ("SetQuery", "Created  Instance %s", instance.toString());

            return (QueryRunnable) instance;
        } catch (ClassNotFoundException e) {
            logger.Print("SetQuery", true, "Class %s not found", queryName);
            logger.error(String.format("SetQuery (Class %s not Found)", queryName), e);
            throw e;
        } catch (NoSuchMethodException e) {
            logger.Print("SetQuery",true,"Constructor not found for %s", queryName);
            logger.error(String.format("SetQuery (Constructor not found for %s)", queryName), e);
            throw e;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            logger.Print("SetQuery",true,"Error instantiating class for %s", queryName);
            logger.error(String.format("SetQuery (Error instantiating class for %s)", queryName), e);
            throw e;
        }
    }

    public static String GetShortClassName(final String className) {
        if(className == null) return "";

        String[] parts = className.trim().split("\\.");
        return parts[parts.length - 1];
    }

    public static String GetShortErrorMsg(final String errMsg) {
        return GetShortErrorMsg(errMsg, 0);
    }

    public static String GetShortErrorMsg(final String errMsg,
                                         final int msgLength) {
        return GetShortErrorMsg(errMsg, msgLength, null, null);
    }

    public static String GetShortErrorMsg(final String errMsg,
                                          final int msgLength,
                                          final String prefix,
                                          final String breakPrefix) {
        if(errMsg == null) return "";

        String[] msgParts = errMsg.split(":");
        StringBuilder sb = new StringBuilder();
        int cnt = 0;

        if(prefix != null) sb.append(prefix);

        for (String msgPart : msgParts) {
            cnt++;
            msgPart = msgPart.trim();
            if(msgPart.startsWith("java.")
                || msgPart.startsWith("com.")
                || msgPart.startsWith("org.")) {
                if(prefix != null) sb.append(prefix);
                sb.append(GetShortClassName(msgPart));
                if(breakPrefix != null) {
                    sb.append(":");
                    sb.append(breakPrefix.repeat(cnt));
                } else {
                    sb.append(": ");
                }
            } else if (msgPart.contains("Please refer to troubleshooting or contact support if problem persists.")) {
                sb.append(msgPart.replaceAll("Please refer to troubleshooting.+problem persists.", " "));
                sb.append(": ");
            }
            else {
                if(prefix != null) sb.append(prefix);
                sb.append(msgPart);
                if(breakPrefix != null) {
                    sb.append(":");
                    sb.append(breakPrefix.repeat(cnt));
                } else {
                    sb.append(": ");
                }
            }
        }

        if(msgLength > 0 && sb.length() >= msgLength) {
            sb.replace(msgLength-3, msgLength, "...");
            sb.setLength(msgLength);
        }

        return sb.toString().trim();
    }

    public static Pair<String,String> GetPidThreadId() {
        String pidString = "<pid>";
        String threadId = "<thread>";
        try {
            RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            String jvmName = runtimeBean.getName();
            pidString = jvmName.split("@")[0]; // Extract PID from the name string
        } catch (Exception ignored) { }
        try {
            threadId = Long.toString(Thread.currentThread().threadId());
        } catch (Exception ignored) { }

        return Pair.with(pidString, threadId);
    }

    private static final Pattern numericPattern = Pattern.compile("^-?\\d+(?<decimal>\\.\\d+)?$");

    public static Pair<Boolean, Boolean> isNumeric(final String strNum) {
        Matcher matcher = null;

        if ((strNum != null
                && !strNum.isEmpty())
                && (matcher = numericPattern.matcher(strNum.trim())).matches()) {
            return Pair.with(true,
                            matcher.group("decimal") != null);
        }

        return Pair.with(false,false);
    }

    public static Object toProperGremlinObject(final String str) {

        Pair<Boolean, Boolean> isNumeric = isNumeric(str);
        if(isNumeric.getValue0()) {
            if(isNumeric.getValue1())
                return Float.parseFloat(str);
            return Integer.parseInt(str);
        }

        return toProperGremlinString(str);
    }

    public static String toProperGremlinString(final String str) {
        if(str == null) return null;

        String trimmedStr = str.trim()
                                .replace("'", "\"");
        if(!trimmedStr.startsWith("\""))
            return String.format("\"%s\"", trimmedStr);
        return str;
    }
}

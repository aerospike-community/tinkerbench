package com.aerospike;

import org.javatuples.Pair;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class Helpers {

    public static final String RESET = "\u001B[0m";
    public static final String BLACK = "\u001B[30m";
    public static final String RED = "\u001B[31m";
    public static final String GREEN = "\u001B[32m";
    public static final String YELLOW = "\u001B[33m";
    public static final String BLUE = "\u001B[34m";
    public static final String PURPLE = "\u001B[35m";
    public static final String CYAN = "\u001B[36m";
    public static final String WHITE = "\u001B[37m";

    // Background colors
    public static final String BLACK_BACKGROUND = "\u001B[40m";
    public static final String RED_BACKGROUND = "\u001B[41m";
    public static final String GREEN_BACKGROUND = "\u001B[42m";
    public static final String YELLOW_BACKGROUND = "\u001B[43m";
    public static final String BLUE_BACKGROUND = "\u001B[44m";
    public static final String PURPLE_BACKGROUND = "\u001B[45m";
    public static final String CYAN_BACKGROUND = "\u001B[46m";
    public static final String WHITE_BACKGROUND = "\u001B[47m";


    public static final double NS_TO_MS = 1000000D;
    public static final double NS_TO_US = 1000D;

    public static void Print(PrintStream printStream,
                             final String msg,
                             final String textColor,
                             final String backgroundColor) {
        try {
            if(backgroundColor != null) {
                printStream.print(backgroundColor);
            }
            printStream.print(textColor);
            printStream.print(msg);
        }
        finally {
            printStream.print(RESET);
        }
    }

    public static void Print(PrintStream printStream,
                             final String msg,
                             final String textColor) {
        Print(printStream, msg, textColor, null);
    }

    public static void Println(PrintStream printStream,
                                 final String msg,
                                 final String textColor,
                                 final String backgroundColor) {
        try {
            if(backgroundColor != null) {
                printStream.print(backgroundColor);
            }
            printStream.print(textColor);
            printStream.print(msg);
        }
        finally {
            printStream.println(RESET);
        }
    }

    public static void Println(PrintStream printStream,
                                 final String msg,
                                 final String textColor) {
        Println(printStream, msg, textColor, null);
    }

    public static QueryRunnable GetQuery(String queryName,
                                         WorkloadProvider provider,
                                         AGSGraphTraversal graphTraversal,
                                         IdManager idManager,
                                         boolean debug) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final LogSource logger = LogSource.getInstance();

        try {

            logger.PrintDebug("GetQuery", "Creating Instance %s", queryName);

            Class<?> queryClass = Class.forName("com.aerospike.predefined." + queryName);
            Constructor<?> constructor = queryClass.getConstructor(WorkloadProvider.class,
                                                                    AGSGraphTraversal.class,
                                                                    IdManager.class);
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

    private static Class<?> getClass(String className, String packageName) {
        try {
            Class<?> possibleClass = Class.forName(packageName + "."
                                                    + className.substring(0, className.lastIndexOf('.')));
            if(QueryRunnable.class.isAssignableFrom(possibleClass)) {
                return possibleClass;
            }
        } catch (ClassNotFoundException ignored) { }
        return null;
    }

    public static Set<Class<?>> findAllClasses(final String packageName) {
        InputStream stream = ClassLoader.getSystemClassLoader()
                .getResourceAsStream(packageName.replaceAll("[.]", "/"));
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return reader.lines()
                .filter(line -> line.endsWith(".class"))
                .map(line -> getClass(line, packageName))
                .collect(Collectors.toSet());
    }

    public static List<String> findAllPredefinedQueries(String packageName) {
        final LogSource logger = LogSource.getInstance();

        Set<Class<?>> classes = Helpers.findAllClasses("com.aerospike.predefined");

        if(!classes.isEmpty()) {
            List<String> queries = new ArrayList<>();
            for (Class<?> c : classes) {
                try {
                    Constructor<?> constructor = c.getConstructor(WorkloadProvider.class,
                                                                    AGSGraphTraversal.class,
                                                                    IdManager.class);
                    Object instance = constructor.newInstance(null, null, null);
                    QueryRunnable query = (QueryRunnable) instance;

                    queries.add(String.format("%s - %s",
                                 query.Name(),
                                 query.getDescription()));

                }
                catch (NoSuchMethodException e) {
                    logger.Print("findAllPredefinedQueries",true,"Constructor not found for Predefined List");
                    logger.error("findAllPredefinedQueries (Constructor not found for Predefined List)", e);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    logger.Print("findAllPredefinedQueries",true,"Error instantiating class for Predefined List");
                    logger.error(String.format("findAllPredefinedQueries (Error instantiating class for %s)", c.getName()), e);
                }
            }

            Collections.sort(queries);
            return queries;
        }

        return  Collections.emptyList();
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

    private static final List<String> validbools = Arrays.asList("true", "false");

    public static Object DetermineValue(final String item) {
        Pair<Boolean, Boolean> isNumeric = isNumeric(item);
        String type = "String";

        if(isNumeric.getValue0()) {
            if(isNumeric.getValue1())
                type = "float";
            else 
                type = "int";
        } else if (validbools.contains(item.toLowerCase())) {
            type = "boolean";
        }

        return DetermineValue(item, type);
    }

    public static Object DetermineValue(final String item,
                                        final String type) {
        return DetermineValue(item, type, null);
    }

    public static Object DetermineValue(final String item,
                                         final String type,
                                         final String subtype) {
        try {
            switch(type.toLowerCase().trim()) {
                case "list": {
                    String[] items = item
                            .substring(1,item.length() - 1)
                            .split(",");
                    List<Object> list = new ArrayList<>();
                    for(String item2 : items) {
                        list.add(DetermineValue(item2, subtype, null));
                    }
                    if(list.isEmpty()
                            || list.stream().anyMatch(Objects::isNull)) {
                        return null;
                    }
                    return list;
                }
                case "string":
                    return item;
                case "float":
                    return Float.parseFloat(item);
                case "double":
                    return Double.parseDouble(item);
                case "integer":
                case "int":
                    return Integer.parseInt(item);
                case "long":
                    return Long.parseLong(item);
                case "boolean":
                case "bool":
                    return Boolean.parseBoolean(item);
                default:
                    return Class.forName(item);
            }

        } catch (Exception ignored) {}
        return null;
    }
}

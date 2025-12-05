package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.javatuples.Pair;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;

import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.*;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;
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
    public static final String LIGHT_RED_BACKGROUND = "\u001B[101m";
    public static final String GREEN_BACKGROUND = "\u001B[42m";
    public static final String LIGHT_GREEN_BACKGROUND = "\u001B[102m";
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

    public static <T, R> List<T> removeDuplicates(List<T> list, Function<T, R> keyExtractor) {
        Set<R> seenKeys = new HashSet<>();
        return list.stream()
                .filter(e -> seenKeys.add(keyExtractor.apply(e)))
                .collect(Collectors.toList());
    }

    public static List<String> getJVMArgs() {
        // Get the RuntimeMXBean, which provides information about the running Java Virtual Machine.
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

        // Get the list of input arguments passed to the JVM.
        return runtimeMxBean.getInputArguments();
    }

    public static String getJarFile(Class<?> aClass) {
        // Get the ProtectionDomain
        final ProtectionDomain protectionDomain = aClass.getProtectionDomain();

        // Get the CodeSource
        final CodeSource codeSource = protectionDomain.getCodeSource();

        // Get the location URL
        if (codeSource != null) {
            final URL location = codeSource.getLocation();
            if (location != null) {
                return location.getPath();
            }
        }
        return null;
    }

    private static final List<Class<?>> PreDefinedClasses = new  ArrayList<Class<?>>();

    public static Class<?> getClass(String jarFilePath, String className) throws ClassNotFoundException {
        try {
            return Class.forName(className);
        } catch (ClassNotFoundException ignore) {
            try {
                URL[] urls = {new URL("jar:file:" + jarFilePath + "!/")};
                try (URLClassLoader cl = URLClassLoader.newInstance(urls)) {
                    return cl.loadClass(className);
                } catch (IOException e) {
                    throw new ClassNotFoundException(className, e);
                }
            } catch (MalformedURLException e) {
                throw new ClassNotFoundException(className, e);
            }
        }
    }

    public static Class<?> getClass(String className) throws ClassNotFoundException {

        if(className.startsWith("com.aerospike.")
                && !className.startsWith("com.aerospike.predefined.")) {
            return Class.forName(className);
        }

        if(PreDefinedClasses.isEmpty()) {
            findAllPredefinedQueries();
        }
        if(PreDefinedClasses.isEmpty()) {
            return Class.forName(className);
        } else {
            Class<?> clazz = PreDefinedClasses.stream().filter(c -> c.getName().equals(className)).findFirst().orElse(null);
            if(clazz != null) {
                return clazz;
            }
        }
        throw new ClassNotFoundException(className);
    }

    public static QueryRunnable GetQuery(Class<?> queryClass,
                                         WorkloadProvider provider,
                                         AGSGraphTraversal graphTraversal,
                                         IdManager idManager,
                                         boolean debug) throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final LogSource logger = LogSource.getInstance();

        try {
            logger.PrintDebug("GetQuery", "Creating Instance %s",
                                queryClass.getName());

            Constructor<?> constructor = queryClass.getConstructor(WorkloadProvider.class,
                                                                    AGSGraphTraversal.class,
                                                                    IdManager.class);
            logger.PrintDebug("GetQuery", "Created  Class %s", queryClass.getName());

            // Instantiate the class by providing arguments to the constructor
            Object instance = constructor.newInstance(provider, graphTraversal, idManager);

            logger.PrintDebug ("SetQuery", "Created  Instance %s", instance.toString());

            return (QueryRunnable) instance;
        } catch (NoSuchMethodException e) {
            logger.Print("SetQuery",true,"Constructor not found for %s", queryClass.getName());
            logger.error(String.format("SetQuery (Constructor not found for %s)", queryClass.getName()), e);
            throw e;
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
            logger.Print("SetQuery",true,"Error instantiating class for %s", queryClass.getName());
            logger.error(String.format("SetQuery (Error instantiating class for %s)", queryClass.getName()), e);
            throw e;
        }
    }


    public static QueryRunnable GetQuery(String queryName,
                                         WorkloadProvider provider,
                                         AGSGraphTraversal graphTraversal,
                                         IdManager idManager,
                                         boolean debug) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final LogSource logger = LogSource.getInstance();

        try {
            logger.PrintDebug("GetQuery", "Creating Instance %s", queryName);

            if(!queryName.startsWith("com.aerospike.predefined.")) {
                if(!queryName.contains("."))
                    queryName = "com.aerospike.predefined." + queryName;
            }

            Class<?> queryClass = getClass(queryName);
            return GetQuery(queryClass,
                            provider,
                            graphTraversal,
                            idManager,
                            debug);
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

    public static List<Class<?>> getClassesInPackage(String packageName) {
        String path = packageName.replaceAll("[.]", "/");
        final List<Class<?>> classes = new ArrayList<>();
        final List<String> classPathEntries = new ArrayList<>();

        {
            final String tbqryjaroverride = System.getProperty("TBQryJarOvr");
            if (tbqryjaroverride != null) {
                classPathEntries.addAll(List.of(tbqryjaroverride.split(
                        File.pathSeparator
                )));
            }
            final String classPath = System.getProperty("java.class.path");
            if (classPath != null) {
                classPathEntries.addAll(List.of(classPath.split(
                        File.pathSeparator
                )));
            }
            final String tbqryjar = System.getProperty("TBQryJar");
            if (tbqryjar != null) {
                classPathEntries.addAll(List.of(tbqryjar.split(
                        File.pathSeparator
                )));
            }
        }

        String name;
        for (String classpathEntry : classPathEntries) {
            if (classpathEntry.endsWith(".jar")) {
                File jar = new File(classpathEntry);
                String classPath = null;
                try {
                    JarInputStream is = new JarInputStream(new FileInputStream(jar));
                    JarEntry entry;
                    while ((entry = is.getNextJarEntry()) != null) {
                        name = entry.getName();
                        if (name.endsWith(".class") && name.contains(path)) {
                            classPath = name.substring(0, entry.getName().length() - 6);
                            classPath = classPath.replaceAll("[\\|/]", ".");
                            try {
                                classes.add(getClass(classpathEntry, classPath));
                            } catch (ClassNotFoundException e1) {
                                String msg = String.format("Class '%s' (%s) not found in JVM. Searching for '%s'",
                                        classPath,
                                        classpathEntry,
                                        packageName);
                                LogSource.getInstance().error(msg, e1);
                                LogSource.getInstance().Print("getClassesInPackage", true, msg);
                            }
                        }
                    }
                } catch (Exception ignored) { }
            } else {
                String classPath = null;
                try {
                    File base = new File(classpathEntry + File.separatorChar + path);
                    for (File file : Objects.requireNonNull(base.listFiles())) {
                        name = file.getName();
                        if (name.endsWith(".class")) {
                            name = name.substring(0, name.length() - 6);
                            classPath = packageName + "." + name;
                            try {
                                classes.add(getClass(classpathEntry, classPath));
                            } catch (ClassNotFoundException e1) {
                                String msg = String.format("Class '%s' (%s) not found in JVM. Searching for '%s'",
                                                            classPath,
                                                            classpathEntry,
                                                            packageName);
                                LogSource.getInstance().error(msg, e1);
                                LogSource.getInstance().Print("getClassesInPackage", true, msg);
                            }
                        }
                    }
                } catch (Exception ignored) { }
            }
        }

        return removeDuplicates(classes, Class::getName);
    }

    public static List<QueryRunnable> findAllPredefinedQueries(String packageName) {
        final LogSource logger = LogSource.getInstance();

        List<Class<?>> classes = getClassesInPackage(packageName);

        if(!classes.isEmpty()) {
            if(logger.isDebug()) {
                logger.PrintDebug("findAllPredefinedQueries",
                                    "Found Classes: %s",
                                        classes.stream()
                                                .map(c -> c.getName() + " -- " + getJarFile(c))
                                                .collect(Collectors.joining(",\n\t")));
            }
            List<QueryRunnable> queries = new ArrayList<>();
            for (Class<?> c : classes) {
                try {
                    Constructor<?> constructor = c.getConstructor(WorkloadProvider.class,
                                                                    AGSGraphTraversal.class,
                                                                    IdManager.class);
                    Object instance = constructor.newInstance(null, null, null);
                    queries.add((QueryRunnable) instance);
                    PreDefinedClasses.add(c);
                }
                catch (NoSuchMethodException e) {
                    logger.Print("findAllPredefinedQueries",true,"Constructor not found for Predefined List");
                    logger.error("findAllPredefinedQueries (Constructor not found for Predefined List)", e);
                } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                    logger.Print("findAllPredefinedQueries",true,"Error instantiating class for Predefined List");
                    logger.error(String.format("findAllPredefinedQueries (Error instantiating class for %s)", c.getName()), e);
                }
            }

            queries.sort(Comparator.comparing(QueryRunnable::Name));
            return queries;
        }

        return  Collections.emptyList();
    }

    public static List<QueryRunnable> findAllPredefinedQueries() {
        return findAllPredefinedQueries("com.aerospike.predefined");
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
                sb.append(msgPart.replaceAll("org\\.apache\\.tinkerpop\\.|com\\.aerospike\\.|java\\.lang\\.|org\\.apache\\.","..."));
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

    public static String getErrorMessage(Throwable error, int depth) {

        if(error == null) { return "<Null>"; }
        String errMsg = error.getMessage();
        if(errMsg == null) {
            if(depth > 50)
                return error.toString();
            if(error instanceof ResponseException re) {
                String msg = re.getMessage();
                Optional<String> value = re.getRemoteStackTrace();
                return String.format("%s: %sRemoteStackTrace: %s",
                        re.getClass().getName(),
                        msg == null ? "" : msg + " ",
                        value.orElse(""));
            }
            return getErrorMessage(error.getCause(), depth + 1);
        }
        return errMsg;
    }

    public static String getErrorMessage(Throwable error) {
        return getErrorMessage(error, 0);
    }

    public static String GetPid() {
        String pidString = "<pid>";
        try {
            RuntimeMXBean runtimeBean = ManagementFactory.getRuntimeMXBean();
            String jvmName = runtimeBean.getName();
            pidString = jvmName.split("@")[0]; // Extract PID from the name string
        } catch (Exception ignored) { }

        return pidString;
    }

    /*
    Returns a pair of strings where the 1st item is the PID and the second is the thread id...
     */
    public static Pair<String,String> GetPidThreadId() {
        String pidString = GetPid();
        String threadId = "<thread>";
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

    /**
     * Replaces the nth occurrence of a substring in a string.
     *
     * @param source The original string.
     * @param target The substring to find.
     * @param replacement The replacement substring.
     * @param occurrence The occurrence number to replace (1 for the first, 2 for the second, etc.).
     * @return A new string with the specified occurrence replaced, or the original string if the occurrence isn't found.
     */
    public static String ReplaceNthOccurrence(String source, String target, String replacement, int occurrence) {
        if (occurrence <= 0) {
            return source;
        }

        int index = -1;
        int count = 0;
        int fromIndex = 0;

        // Loop to find the index of the nth occurrence
        while (count < occurrence && (index = source.indexOf(target, fromIndex)) != -1) {
            count++;
            if (count == occurrence) {
                // Once the nth occurrence is found, break the loop
                break;
            }
            // Update the starting index for the next search
            fromIndex = index + target.length();
        }

        if (index == -1) {
            // If the nth occurrence is not found, return the original string
            return source;
        }

        // Reconstruct the string using substring and the replacement
        // Java Strings are immutable, so a new String is created
        return source.substring(0, index) + replacement + source.substring(index + target.length());
    }

    public static String[] TrimTrailingEmptyOrNull(String[] array) {
        if (array == null) {
            return null;
        }

        int newLength = array.length;
        // Iterate backward to find the first non-null, non-empty element
        for (int i = array.length - 1; i >= 0; i--) {
            if (array[i] == null || array[i].isEmpty()) {
                newLength--;
            } else {
                break; // Stop when a valid element is found
            }
        }

        // Copy the valid part of the array into a new, smaller array
        return Arrays.copyOf(array, newLength);
    }

    /*
    *   If object is an Optional instance, it will unwrap it and return the actual value or null.
    *   @param obj -- Possible Optional instance
    *   @return the actual value or null, if not present...
     */
    public static Object Unwrap(Object obj) {
        if (obj instanceof Optional<?> opt) {
            return opt.orElse(null);
        }
        return obj;
    }

    public static ZonedDateTime GetLocalTimeZone(final LocalDateTime localDateTime) {
        return localDateTime.atZone(ZoneId.systemDefault());
    }

    public static ZonedDateTime GetUTC(final LocalDateTime localDateTime) {
        final ZonedDateTime zonedDateTime = GetLocalTimeZone(localDateTime);
        return zonedDateTime.withZoneSameInstant(ZoneOffset.UTC);
    }

    static DateTimeFormatter dtFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static String GetUTCString(final LocalDateTime localDateTime) {
        final ZonedDateTime utcDateTime = GetUTC(localDateTime);
        return utcDateTime.format(dtFormatter);
    }

    public static String GetLocalTimeZoneString(final LocalDateTime localDateTime) {
        final ZonedDateTime localZone = GetLocalTimeZone(localDateTime);
        return localZone.format(dtFormatter);
    }

    public static String GetLocalZoneString() {
       return ZoneId.systemDefault().toString();
    }

    public static String GetDateTimeString(final LocalDateTime localDateTime) {
        return localDateTime.format(dtFormatter);
    }

    public static String PrintGrafanaRangeJson(LocalDateTime startDateTime,
                                                LocalDateTime stopDateTime) {
        if(startDateTime != null && stopDateTime != null) {
            //{"from":"2025-08-26 16:11:19","to":"2025-08-26 16:12:52"}
            String dt = String.format("\tGrafana UTC Date Range: {\"from\":\"%s\",\"to\":\"%s\"}",
                                        GetUTCString(startDateTime),
                                        GetUTCString(stopDateTime));

            if(!GetLocalZoneString().equals("UTC")) {
                dt += String.format("%n\t\t%s Date Range: {\"from\":\"%s\",\"to\":\"%s\"}",
                                    GetLocalZoneString(),
                                    GetLocalTimeZoneString(startDateTime),
                                    GetLocalTimeZoneString(stopDateTime));
            }
            return dt;
        }
        return null;
    }

    public static <T,V> void UpdateField(T instance,
                                             String fieldName,
                                             V value,
                                             boolean fieldIsPrivate) throws NoSuchFieldException, IllegalAccessException {

        final Field field = instance.getClass().getField( fieldName);
        if(fieldIsPrivate) {
            field.setAccessible(true);
        }
        field.set(instance, value); // Update the value
    }

    public static <T,V> void UpdateFieldSetter(T instance,
                                                 String setterName,
                                                 V value,
                                                 boolean fieldIsPrivate) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IllegalArgumentException {

        final Method[] setters = instance.getClass().getMethods();

        for(Method setter : Arrays.stream(setters)
                                .filter(i -> i.getParameterCount() == 1)
                                .toList()) {
            if(setter.getName().equals(setterName)) {
                if(fieldIsPrivate) {
                    setter.setAccessible(true);
                }
                setter.invoke(instance, value);
                return;
            }
        }
        throw new NoSuchMethodException(String.format("Setter %s not found", setterName));
    }

    /*
        value being rounded
        scale is the Significant Digits represented as that number to the power of 10.
            Example:
                Significant Digits = 3
                Scale = 10^3 (1000)
     */
    public static double RoundNumberOfSignificantDigitsScale(double value, double scale) {
       return Math.round(value * scale) / scale;
    }

    public static double RoundNumberOfSignificantDigits(double value, int significantDigits) {
        if(significantDigits == 0) return Math.round(value);

        final double scale = Math.pow(10, significantDigits);
        return Math.round(value * scale) / scale;
    }

    public static String FmtDuration(Duration duration) {
        final long hours = duration.toHours();
        final int minutes = duration.toMinutesPart();
        final int seconds = duration.toSecondsPart();
        final int milliseconds = duration.toMillisPart();

        if(hours > 0) {
            return String.format("%02dH%02dM%02d.%03dS",
                                    hours,
                                    minutes,
                                    seconds,
                                    milliseconds);
        }
        if(minutes > 0) {
            return String.format("%02dM%02d.%03dS",
                                    minutes,
                                    seconds,
                                    milliseconds);
        }
        if(seconds > 0) {
            return String.format("%02d.%03dS",
                    seconds,
                    milliseconds);
        }
        return String.format("%,dMS", milliseconds);
    }

    public static String FmtInt(int value) {
        if(value >= 1_000_000_000) {
            return String.format("%,.3fG",
                    RoundNumberOfSignificantDigits((double) value/1_000_000_000.0, 3));
        }
        if(value >= 1000000) {
            return String.format("%,.3fM",
                    RoundNumberOfSignificantDigits((double) value/1000000.0, 3));
        }
        if(value >= 1000) {
            return String.format("%,.3fK",
                    RoundNumberOfSignificantDigits((double) value/1000.0, 3));
        }
        return String.format("%,d", value);
    }

    public static boolean hasWildcard(String filePath) {
        if (filePath == null || filePath.isEmpty()) {
            return false;
        }
        return filePath.contains("*") || filePath.contains("?");
    }

    public static List<File> GetFiles(String startDir, String wildcardPattern) {

        if (wildcardPattern == null || wildcardPattern.isEmpty()) {
            return Collections.emptyList();
        }

        Path startPath;

        if(startDir == null) {
            File globFile = new File(wildcardPattern);
            File parent = globFile.getParentFile();
            if(parent != null && parent.exists()) {
                startPath = parent.toPath();
                wildcardPattern = globFile.getName();
            } else {
                startPath = Paths.get(System.getProperty("user.dir"));
            }
        } else {
            startPath = Paths.get(startDir);
        }

        List<Path> matchingFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(startPath, wildcardPattern)) {
            for (Path entry : stream) {
                matchingFiles.add(entry);
            }
        } catch (IOException e) {
            LogSource.getInstance().error(String.format("Error while trying to list files in path '%s' for pattern '%s'",
                                                            startDir,
                                                            wildcardPattern),
                                            e);
        }

        if(LogSource.getInstance().isDebug()) {
            for (Path file : matchingFiles) {
                String msg = String.format("Found file '%s'", file.getFileName());
                LogSource.getInstance().PrintDebug("GetFiles",  msg);
            }
        }

        return matchingFiles.stream()
                                .map(Path::toFile)
                                .collect(Collectors.toList());
    }

    public static File CrateFolderFilePath(String filePath) {
        File file = new File(filePath);

        if(file.exists()) {
            return file;
        }

        File directoryFile = file.getParentFile();

        if(directoryFile == null) {
            return file;
        }

        try {
            if (!directoryFile.exists()) {
                if(directoryFile.mkdirs()) {
                    return file;
                }
                throw new IOException("Unable to create directory " + directoryFile);
            }

        } catch (Exception e) {
            LogSource.getInstance().error(String.format("Error while trying to create the folder '%s'",
                            directoryFile),
                    e);
        }

        return file;
    }
}

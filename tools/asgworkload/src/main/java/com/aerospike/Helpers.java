package com.aerospike;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

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
        if(errMsg == null) return "";

        String[] msgParts = errMsg.split(":");
        StringBuilder sb = new StringBuilder();

        for (String msgPart : msgParts) {
            msgPart = msgPart.trim();
            if(msgPart.startsWith("java.")
                || msgPart.startsWith("com.")
                || msgPart.startsWith("org.")) {
                sb.append(GetShortClassName(msgPart));
                sb.append(": ");
            } else if (msgPart.contains("Please refer to troubleshooting or contact support if problem persists.")) {
                sb.append(msgPart.replaceAll("Please refer to troubleshooting.+problem persists.", " "));
                sb.append(": ");
            }
            else {
                sb.append(msgPart);
                sb.append(": ");
            }
        }

        if(msgLength > 0 && sb.length() >= msgLength) {
            sb.replace(msgLength-3, msgLength, "...");
            sb.setLength(msgLength);
        }

        return sb.toString().trim();
    }
}

package com.aerospike;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public class Helpers {

    public static QueryRunnable GetQuery(String queryName,
                                         WorkloadProvider provider,
                                         AGSGraphTraversal graphTraversal,
                                         boolean debug) throws ClassNotFoundException, NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        final LogSource logger = LogSource.getInstance();

        try {

            logger.PrintDebug("GetQuery", "Creating Instance %s", queryName);

            Class<?> queryClass = Class.forName("com.aerospike." + queryName);
            Constructor<?> constructor = queryClass.getConstructor(WorkloadProvider.class, AGSGraphTraversal.class);
            logger.PrintDebug("GetQuery", "Created  Class %s", queryClass.getName());

            // Instantiate the class by providing arguments to the constructor
            Object instance = constructor.newInstance(provider, graphTraversal);

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

}

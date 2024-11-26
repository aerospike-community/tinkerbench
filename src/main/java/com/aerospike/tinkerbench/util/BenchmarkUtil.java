package com.aerospike.tinkerbench.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class BenchmarkUtil {

    public static Configuration loadConfig(final Path path) {
        System.out.println("Loading file " + path.getFileName());
        try {
            final Properties props = new Properties();
            props.load(Files.newBufferedReader(path));

            final HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                configData.put(key, value);
            });

            return new MapConfiguration(configData);
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Configuration config;

    private static String readProperty(final String propertyName) {
        // Let system properties override config file.
        if (System.getenv(propertyName) != null) {
            return System.getenv(propertyName);
        }

        if (config != null) {
            return config.getString(propertyName.toLowerCase());
        }

        final String propertyFile = System.getProperty("config");
        if (propertyFile == null || propertyFile.isEmpty()) {
            return System.getProperty(propertyName);
        }

        try {
            config = loadConfig(Path.of(propertyFile));
            return config.getString(propertyName.toLowerCase());
        } catch (final Exception e) {
            throw new RuntimeException("Failed to load config file", e);
        }
    }

    private static final String LOCALHOST = "localhost";
    public static final int DEFAULT_PORT = 8182;

    public static String[] getHost() {
        final String hosts = readProperty("graph.server.host");
        if (hosts == null) {
            System.out.println("No 'graph.server.host' system property set. Defaulting to localhost.");
            return new String[] { LOCALHOST };
        }
        final String[] hostsArray = hosts.split(",");
        for (int i = 0; i < hostsArray.length; i++) {
            hostsArray[i] = hostsArray[i].trim();
        }
        return hostsArray;
    }

    public static int getPort() {
        try {
            final String port = readProperty("graph.server.port");
            if (port == null) {
                System.out.println("No 'graph.server.port' system property set. Defaulting to " + DEFAULT_PORT + ".");
                return DEFAULT_PORT;
            }
            return Integer.parseInt(port);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get port from system property 'graph.server.port'. Value provided: '" +
                            readProperty("graph.server.port") + "'.");
        }
    }

    public static int getMaxConnectionPoolSize() {
        try {
            final String maxConnectionPoolSize = readProperty("graph.client.maxConnectionPoolSize");
            if (maxConnectionPoolSize == null) {
                System.out.println("No 'graph.client.maxConnectionPoolSize' system property set. Defaulting to 8.");
                return 8;
            }
            return Integer.parseInt(maxConnectionPoolSize);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get maxConnectionPoolSize from system property 'graph.client.maxConnectionPoolSize'. Value provided: '" +
                            readProperty("graph.client.maxConnectionPoolSize") + "'.");
        }
    }
// Create a function that retrieves graph.client user and graph.client.password from the properties file
    public static String getUser() {
        final String user = readProperty("graph.client.user");
        if (user == null) {
            System.out.println("No 'graph.client.user' system property set. Defaulting to null.");
            return null;
        }
        return user;
    }

    public static String getPassword() {
        final String password = readProperty("graph.client.password");
        if (password == null) {
            System.out.println("No 'graph.client.password' system property set. Defaulting to null.");
            return null;
        }
        return password;
    }

    public static int getMinConnectionPoolSize() {
        try {
            final String minConnectionPoolSize = readProperty("graph.client.minConnectionPoolSize");
            if (minConnectionPoolSize == null) {
                System.out.println("No 'graph.client.minConnectionPoolSize' system property set. Defaulting to 8.");
                return 8;
            }
            return Integer.parseInt(minConnectionPoolSize);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get minConnectionPoolSize from system property 'graph.client.maxConnectionPoolSize'. Value provided: '" +
                            readProperty("graph.client.maxConnectionPoolSize") + "'.");
        }
    }

    public static int getMaxInProcessPerConnection() {
        try {
            final String maxInProcessPerConnection = readProperty("graph.client.maxInProcessPerConnection");
            if (maxInProcessPerConnection == null) {
                System.out.println("No 'graph.client.maxInProcessPerConnection' system property set. Defaulting to 8.");
                return 8;
            }
            return Integer.parseInt(maxInProcessPerConnection);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get maxInProcessPerConnection from system property 'graph.client.maxInProcessPerConnection'. Value provided: '" +
                            readProperty("graph.client.maxInProcessPerConnection") + "'.");
        }
    }

    public static int getMaxSimultaneousUsagePerConnection() {
        try {
            final String maxSimultaneousUsagePerConnection = readProperty("graph.client.maxSimultaneousUsagePerConnection");
            if (maxSimultaneousUsagePerConnection == null) {
                System.out.println("No 'graph.client.maxSimultaneousUsagePerConnection' system property set. Defaulting to 8.");
                return 8;
            }
            return Integer.parseInt(maxSimultaneousUsagePerConnection);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get maxSimultaneousUsagePerConnection from system property 'graph.client.maxSimultaneousUsagePerConnection'. Value provided: '" +
                            readProperty("graph.client.maxSimultaneousUsagePerConnection") + "'.");
        }
    }

    public static int getMinSimultaneousUsagePerConnection() {
        try {
            final String minSimultaneousUsagePerConnection = readProperty("graph.client.minSimultaneousUsagePerConnection");
            if (minSimultaneousUsagePerConnection == null) {
                System.out.println("No 'graph.client.minSimultaneousUsagePerConnection' system property set. Defaulting to 8.");
                return 8;
            }
            return Integer.parseInt(minSimultaneousUsagePerConnection);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get maxSimultaneousUsagePerConnection from system property 'graph.client.maxSimultaneousUsagePerConnection'. Value provided: '" +
                            readProperty("graph.client.maxSimultaneousUsagePerConnection") + "'.");
        }
    }

    public static boolean getSSL() {
        try {
            final String ssl = readProperty("graph.client.ssl");
            if (ssl == null) {
                System.out.println("No 'graph.client.ssl' system property set. Defaulting to false.");
                return false;
            }
            return Boolean.parseBoolean(ssl);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get ssl from system property 'graph.client.ssl' 'true' or 'false' expected. Value provided: '" +
                            readProperty("graph.client.ssl") + "'.");
        }
    }

    public static int getMeasurementForks() {
        try {
            final String measurementForks = readProperty("benchmark.measurementForks");
            if (measurementForks == null) {
                System.out.println("No 'benchmark.measurementForks' system property set. Defaulting to 4 forks.");
                return 4;
            }
            return Integer.parseInt(measurementForks);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get measurementForks from system property 'benchmark.measurementForks'. Value provided: '" +
                            readProperty("benchmark.measurementForks") + "'.");
        }
    }

    public static int getMeasurementIterations() {
        try {
            final String measurementIterations = readProperty("benchmark.measurementIterations");
            if (measurementIterations == null) {
                System.out.println("No 'benchmark.measurementIterations' system property set. Defaulting to 5 iterations.");
                return 5;
            }
            return Integer.parseInt(measurementIterations);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get measurementIterations from system property 'benchmark.measurementIterations'. Value provided: '" +
                            readProperty("benchmark.measurementIterations") + "'.");
        }
    }

    public static int getMeasurementTime() {
        try {
            final String measurementTime = readProperty("benchmark.measurementTime");
            if (measurementTime == null) {
                System.out.println("No 'benchmark.measurementTime' system property set. Defaulting to 5 seconds.");
                return 5;
            }
            return Integer.parseInt(measurementTime);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get measurementTime from system property 'benchmark.measurementTime'. Value provided: '" +
                            readProperty("benchmark.measurementTime") + "'.");
        }
    }

    public static int getMeasurementTimeout() {
        try {
            final String measurementTimeout = readProperty("benchmark.measurementTimeout");
            if (measurementTimeout == null) {
                System.out.println("No 'benchmark.measurementTimeout' system property set. Defaulting to 5 seconds.");
                return 5;
            }
            return Integer.parseInt(measurementTimeout);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get measurementTimeout from system property 'benchmark.measurementTimeout'. Value provided: '" +
                            readProperty("benchmark.measurementTimeout") + "'.");
        }
    }

    public static int getMeasurementThreads() {
        try {
            final String measurementThreads = readProperty("benchmark.measurementThreads");
            if (measurementThreads == null) {
                System.out.println("No 'benchmark.measurementThreads' system property set. Defaulting to 4 threads.");
                return 4;
            }
            return Integer.parseInt(measurementThreads);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get measurementThreads from system property 'benchmark.measurementThreads'. Value provided: '" +
                            readProperty("benchmark.measurementThreads") + "'.");
        }
    }

    public static Mode getMode() {
        final String mode = readProperty("benchmark.mode");
        final Mode benchmarkMode;
        if (mode == null) {
            benchmarkMode = Mode.AverageTime;
        } else if ("all".equalsIgnoreCase(mode)) {
            System.out.println("Setting mode to 'All'.");
            benchmarkMode = Mode.All;
        } else if ("throughput".equalsIgnoreCase(mode)) {
            System.out.println("Setting mode to 'Throughput'.");
            benchmarkMode = Mode.Throughput;
        } else if ("average".equalsIgnoreCase(mode)) {
            System.out.println("Setting mode to 'Average'.");
            benchmarkMode = Mode.AverageTime;
        } else if ("sample".equals(mode)) {
            System.out.println("Setting mode to 'Sample'.");
            benchmarkMode = Mode.SampleTime;
        } else {
            throw new RuntimeException("Error, could not get mode from system property 'benchmark.mode'. " +
                    "Valid values are: 'all', 'throughput', 'average'. Value provided: '" + mode + "'.");
        }
        return benchmarkMode;
    }

    public static int getBenchmarkIdBufferSize() {
        try {
            final String idBufferSize = readProperty("benchmark.idBufferSize");
            if (idBufferSize == null) {
                System.out.println("No 'benchmark.idBufferSize' system property set. Defaulting to 5000.");
                return 5000;
            }
            return Integer.parseInt(idBufferSize);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get integer read limit from system property 'benchmark.idBufferSize'. Value provided: '" +
                            readProperty("benchmark.idBufferSize") + "'.");
        }
    }

    public static int getBenchmarkSeedSize() {
        try {
            final String idBufferSize = readProperty("benchmark.seedSize");
            if (idBufferSize == null) {
                System.out.println("No 'benchmark.seedSize' system property set. Defaulting to 50000.");
                return 50000;
            }
            return Integer.parseInt(idBufferSize);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get integer read limit from system property 'benchmark.seedSize'. Value provided: '" +
                            readProperty("benchmark.seedSize") + "'.");
        }
    }

    public static int getBenchmarkSeedRuntimeMultiplier() {
        try {
            final String idBufferSize = readProperty("benchmark.seedSize.multiplier");
            if (idBufferSize == null) {
                System.out.println("No 'benchmark.seedSize.multiplier' system property set. Defaulting to 4.");
                return 4;
            }
            return Integer.parseInt(idBufferSize);
        } catch (final NumberFormatException e) {
            throw new RuntimeException(
                    "Error, could not get integer read limit from system property 'benchmark.seedSize.multiplier'. Value provided: '" +
                            readProperty("benchmark.seedSize") + "'.");
        }
    }

    public static String getParamQueryID() {
        final String queryID = readProperty("benchmark.param.queryID");
        if (queryID == null) {
            System.out.println("No 'benchmark.param.queryID' property set. Defaulting to null.");
            return null;
        }
        return queryID;
    }

    public static void printUsage() {
        System.out.println("""
                Usage: java -jar <jarfile> <benchmark name options='BenchmarkStitching', 'BenchmarkShortRead'>
                \t-Dgraph.server.host=<graph service host default=localhost>
                \t-Dgraph.server.port=<graph service port default=8182>
                \t-Dgraph.client.maxConnectionPoolSize=<graph client maxConnectionPoolSize default=8>
                \t-Dgraph.client.maxInProcessPerConnection=<graph client maxInProcessPerConnection default=8>
                \t-Dgraph.client.ssl=<graph client ssl enable default=false>
                \t-Dbenchmark.measurementForks=<benchmark measurementForks default=4>
                \t-Dbenchmark.measurementIterations=<benchmark measurementIterations default=5>
                \t-Dbenchmark.measurementTime=<benchmark measurementTime default=5 unit=seconds>
                \t-Dbenchmark.measurementTimeout=<benchmark measurementTimeout default=5 unit=seconds>
                \t-Dbenchmark.measurementThreads=<benchmark measurementThreads default=4>
                \t-Dbenchmark.mode=<benchmark mode default=AverageTime, options='all', 'throughput', 'average', 'sample'>
                \t-benchmark.idBufferSize=<how many ids to buffer for each type of label default=5000>
                """);
    }

    public static void printSummary() {
        System.out.println("Creating the GraphTraversalSource.");
        final Cluster cluster = Cluster.build().addContactPoints(getHost())
                .port(getPort())
                .maxConnectionPoolSize(getMaxConnectionPoolSize())
                .maxInProcessPerConnection(getMaxInProcessPerConnection())
                .enableSsl(getSSL()).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        final Object summary = g.call("summary").with("pretty").next();
        System.out.println("Graph summary: " + summary);
    }

    public static void createIndexes() {
        System.out.println("Creating the GraphTraversalSource.");
        final Cluster cluster = Cluster.build().addContactPoints(getHost())
                .port(getPort())
                .maxConnectionPoolSize(getMaxConnectionPoolSize())
                .maxInProcessPerConnection(getMaxInProcessPerConnection())
                .enableSsl(getSSL()).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster));
        try {
            Object outputCreatePhone = g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "phone_number_hash").next();
            System.out.println("Create phone index: " + outputCreatePhone);
        } catch (final Exception e) {
            System.out.println("Failed to create phone index: " + e.getMessage());
        }
        try {
            Object outputCreateEmail = g.call("aerospike.graph.admin.index.create").
                    with("element_type", "vertex").
                    with("property_key", "email_hash").next();
            System.out.println("Create email index: " + outputCreateEmail);
        } catch (final Exception e) {
            System.out.println("Failed to create email index: " + e.getMessage());
        }
    }

    public static void collectBenchmarkLabelIdMapping(final GraphTraversalSource g,
                                                      final Set<String> labels,
                                                      final Map<String, List<Object>> labelToId) {
        for (final String label : labels) {
            final List<Object> ids = g.V().hasLabel(label).id().limit(BenchmarkUtil.getBenchmarkIdBufferSize()).toList();
            labelToId.put(label, ids);
        }
    }

    public static void seedGraph(final GraphTraversalSource g, final int seedSize) {
        System.out.println("Seeding the graph with " + seedSize + " vertices.");
        final ExecutorService service = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        final int seedCountPerThread = seedSize / Runtime.getRuntime().availableProcessors();
        final List<Future<?>> futureList = new ArrayList<>();
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            final int finalI = i;
            futureList.add(service.submit(() -> {
                System.out.println("Thread " + finalI + " vertex seeding the graph.");
                for (int j = 0; j < seedCountPerThread; j++) {
                    g.addV("vertex_label").
                            property(T.id, finalI * seedCountPerThread + j).
                            property("property_key", "property_value").
                            next();
                }
                System.out.println("Thread " + finalI + " completed vertex seeding the graph.");
            }));
        }

        // Add remaining ids since the division may not be even.
        final int remaining = seedSize % Runtime.getRuntime().availableProcessors();
        for (int i = 0; i < remaining; i++) {
            g.addV("vertex_label").
                    property(T.id, seedSize - remaining + i).
                    property("property_key", "property_value").
                    next();
        }

        for (final Future<?> future : futureList) {
            try {
                future.get();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        // add some edges to this
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            final int finalI = i;
            futureList.add(service.submit(() -> {
                System.out.println("Thread " + finalI + " edge seeding the graph.");
                final Random random = new Random();
                for (int j = 0; j < seedCountPerThread; j++) {
                    g.addE("edge_label").from(__.V(random.nextInt(seedSize))).to(__.V(random.nextInt(seedSize))).
                            property("property_key", "property_value").
                            next();
                }
                System.out.println("Thread " + finalI + " completed edge seeding the graph.");
            }));
        }
        try {
            service.shutdown();
            if (!service.awaitTermination(1, TimeUnit.HOURS)) {
                throw new RuntimeException("Never completed seeding the graph.");
            }
        } catch (final InterruptedException e) {
            // Should never happen.
            throw new RuntimeException(e);
        }
        System.out.println("Completed seeding the graph.");
    }

    public static void collectBenchmarkPropertyLabelMapping(final GraphTraversalSource g,
                                                              final Set<String> properties,
                                                              final String label,
                                                              final Map<String, List<Object>> propertyToId) {
        final List<Map<Object, Object>> dataMap = g.V().hasLabel(label).valueMap().limit(BenchmarkUtil.getBenchmarkIdBufferSize()).toList();
        for (final Map<Object, Object> data: dataMap) {
            for (final String property: properties) {
                propertyToId.computeIfAbsent(property, k -> new ArrayList<>());
                if (data.get(property) instanceof List) {
                    final List<Object> propertyList = (List<Object>) data.get(property);
                    propertyToId.get(property).add(propertyList.get(0));
                } else {
                    propertyToId.get(property).add(data.get(property));
                }
            }
        }
    }

    public static void runBenchmark(final Class<?> clazz) throws RunnerException {
        final List<String> args = new ArrayList<>();
        args.add("-Dgraph.server.host=" + Arrays.toString(getHost()));
        args.add("-Dgraph.server.port=" + getPort());
        args.add("-Dgraph.client.user=" + getUser());
        args.add("-Dgraph.client.password=" + getPassword());
        args.add("-Dgraph.client.maxConnectionPoolSize=" + getMaxConnectionPoolSize());
        args.add("-Dgraph.client.maxInProcessPerConnection=" + getMaxInProcessPerConnection());
        args.add("-Dgraph.client.minConnectionPoolSize=" + getMinConnectionPoolSize());
        args.add("-Dgraph.client.maxSimultaneousUsagePerConnection=" + getMaxSimultaneousUsagePerConnection());
        args.add("-Dgraph.client.minSimultaneousUsagePerConnection=" + getMinSimultaneousUsagePerConnection());
        args.add("-Dgraph.client.ssl=" + getSSL());
        args.add("-Dbenchmark.idBufferSize=" + getBenchmarkIdBufferSize());
        if (System.getProperty("config") != null) {
            args.add("-Dconfig=" + System.getProperty("config"));
        }
        final String[] argsArray = args.toArray(new String[0]);
        final ChainedOptionsBuilder optBuilder = new OptionsBuilder()
                .include(clazz.getSimpleName())
                .detectJvmArgs()
                .threads(getMeasurementThreads())
                .forks(getMeasurementForks())
                .mode(getMode())
                .timeUnit(TimeUnit.MILLISECONDS)
                .measurementIterations(getMeasurementIterations())
                .measurementTime(TimeValue.seconds(getMeasurementTime()))
                .timeout(TimeValue.seconds(getMeasurementTimeout()))
                .jvmArgs(argsArray);

        String queryID = getParamQueryID();
        if (queryID != null) {
            optBuilder.param("queryID", queryID);
        }

        Options opt = optBuilder.build();
        Collection<RunResult> runResult = new Runner(opt).run();
        final List<Map<String, Object>> root = new ArrayList<>();
        runResult.forEach(result -> {
            Map<String, Object> obj = new HashMap<>();
            obj.put("name", result.getPrimaryResult().getLabel());
            obj.put("unit", result.getPrimaryResult().getScoreUnit());
            obj.put("value", result.getPrimaryResult().getScore());
            obj.put("range", result.getPrimaryResult().getScoreError());
            obj.put("extra", "\n\t" + result.getPrimaryResult().getStatistics());
            root.add(obj);
        });
        root.forEach(resultMap -> {
            System.out.println("result: ");
            resultMap.forEach((k, v) -> {
                System.out.printf("\t %s:  %s", k, v);
                System.out.println();
            });
        });
        try (final FileWriter file = new FileWriter(String.format("benchmark-result-%s.json", System.currentTimeMillis()))) {
            System.out.println("Writing json file");
            file.write(root.toString());
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }
}

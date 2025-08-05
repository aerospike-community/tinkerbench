package com.aerospike.tinkerbench.gremlin;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.ResultSet;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

import javax.script.Bindings;
import java.io.FileWriter;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * Simple Gremlin Query Client
 * Connects to a Gremlin server and executes queries from command line arguments
 */
public class GremlinQueryClient {

    public static void main(String[] args) {

        try (final FileWriter file = new FileWriter(String.format("benchmark-result-%s.json", System.currentTimeMillis()))) {
            System.out.println("Writing json file");
            file.write("f");
        } catch (final Exception e) {
            e.printStackTrace();
        }
        if (args.length != 8) {
            System.out.println("Usage: java -jar <jar> --url <server-url> --query <query> [--count <count>] [--warmup <warmupCount>]");
            System.exit(1);
        }

        String serverUrl = null;
        String query = null;
        int count = -1;
        int warmupCount = -1;

        // Parse command line arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--url":
                    if (i + 1 < args.length) {
                        serverUrl = args[++i];
                    } else {
                        System.err.println("Error: --url requires a value");
                        System.exit(1);
                    }
                    break;
                case "--query":
                    if (i + 1 < args.length) {
                        query = args[++i];
                    } else {
                        System.err.println("Error: --query requires a value");
                        System.exit(1);
                    }
                    break;
                case "--count":
                    if (i + 1 < args.length) {
                        try {
                            count = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: --count must be an integer");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Error: --count requires a value");
                        System.exit(1);
                    }
                    break;
                case "--warmup":
                    if (i + 1 < args.length) {
                        try {
                            warmupCount = Integer.parseInt(args[++i]);
                        } catch (NumberFormatException e) {
                            System.err.println("Error: --warmup must be an integer");
                            System.exit(1);
                        }
                    } else {
                        System.err.println("Error: --warmup requires a value");
                        System.exit(1);
                    }
                    break;
                default:
                    System.err.println("Error: Unknown argument: " + args[i]);
                    System.exit(1);
            }
        }
        // Validate required arguments
        // If serverUrl or query is null, exit
        if (serverUrl == null || query == null) {
            System.err.println("Error: Both --url and --query are required");
            System.exit(1);
        }
        if (count < 0 || warmupCount < 0) {
            System.err.println("Error: --count and --warmup must be a non-negative integer");
            System.exit(1);
        }

        final String[] queries = query.split("\\|");
        final Cluster cluster = getCluster(serverUrl);
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        final GraphTraversalSource g = traversal().withRemote(connection).withComputer();
        try {
            final Map<String, List<Duration>> warmupResults = new HashMap<>();
            for (String actualQuery : queries) {
                if (actualQuery.contains(".withComputer")) {
                    // Remove withComputer piece
                    actualQuery = actualQuery.replace(".withComputer()", "");
                }
                for (int i = 0; i < warmupCount; i++) {
                    final Duration d = performQuery(g, actualQuery);
                    if (d != null) {
                        warmupResults.computeIfAbsent(actualQuery, k -> new java.util.ArrayList<>()).add(d);
                    }
                }
            }
            printResults(warmupResults, true);

            final Map<String, List<Duration>> actualResults = new HashMap<>();
            for (String actualQuery : queries) {
                if (actualQuery.contains(".withComputer")) {
                    // Remove withComputer piece
                    actualQuery = actualQuery.replace(".withComputer()", "");
                }
                for (int i = 0; i < count; i++) {
                    final Duration d = performQuery(g, actualQuery);
                    if (d != null) {
                        actualResults.computeIfAbsent(actualQuery, k -> new java.util.ArrayList<>()).add(d);
                    }
                }
            }
            printResults(actualResults, false);

            // Log to file.
            System.out.println("Generation output file with results.");
            createOutputFile(actualResults);
            System.out.println("Generated output file.");
        } catch (final Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            cluster.close();
        }
    }

    private static void createOutputFile(final Map<String, List<Duration>> runResult) {
        final List<Map<String, Object>> root = new ArrayList<>();
        for (final Map.Entry<String, List<Duration>> entry : runResult.entrySet()) {
            // Get error and statistics for each query
            String query = entry.getKey();
            List<Duration> durations = entry.getValue();
            if (durations.isEmpty()) {
                System.out.println("No durations recorded for query: " + query);
                continue;
            }
            long totalMillis = durations.stream().mapToLong(Duration::toSeconds).sum();
            long averageMillis = totalMillis / durations.size();
            long maxMillis = durations.stream().mapToLong(Duration::toSeconds).max().orElse(0);
            long minMillis = durations.stream().mapToLong(Duration::toSeconds).min().orElse(0);

            // get mean error at 999 confidence level
            double meanError = (maxMillis - minMillis) / 2.0;
            double average = (double) averageMillis;
            Map<String, Object> obj = new HashMap<>();
            obj.put("name", query);
            obj.put("unit", "s");
            obj.put("value", average);
            obj.put("range", meanError);
            obj.put("extra", "");
            root.add(obj);
        }
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

    private static void printResults(final Map<String, List<Duration>> results, boolean isWarmup) {
        System.out.println("---------------------------");
        System.out.println(isWarmup ? "Warmup Results:" : "Actual Results:");
        System.out.println("----------------------------");
        for (Map.Entry<String, List<Duration>> entry : results.entrySet()) {
            System.out.println("Query: " + entry.getKey());
            List<Duration> durations = entry.getValue();
            if (durations.isEmpty()) {
                System.out.println("\tNo durations recorded.");
                continue;
            }
            long totalMillis = durations.stream().mapToLong(Duration::toSeconds).sum();
            System.out.println("\tTotal time: " + totalMillis + " s");
            System.out.println("\tAverage time: " + (totalMillis / durations.size()) + " s");
            System.out.println("\tMax time: " + durations.stream().mapToLong(Duration::toSeconds).max().orElse(0) + " s");
            System.out.println("\tMin time: " + durations.stream().mapToLong(Duration::toSeconds).min().orElse(0) + " s");
        }
    }

    private static Duration performQuery(final GraphTraversalSource g, final String query) {
        try {
            String terminator = ".toList()";
            String actualQuery = query;
            if (query.endsWith(".toList()") || query.endsWith(".next()") || query.endsWith(".iterate()") || query.endsWith(".toSet()")) {
                terminator = query.substring(query.lastIndexOf('.'));
                actualQuery = query.substring(0, query.lastIndexOf('.'));
            }
            final GremlinLangScriptEngine gremlinLangEngine = new GremlinLangScriptEngine();
            final Bindings bindings = gremlinLangEngine.createBindings();
            bindings.put("g", g);
            final Traversal t = ((DefaultGraphTraversal<?, ?>) gremlinLangEngine.eval(actualQuery, bindings));
            System.out.println("Running query: " + query + terminator);
            Instant start = Instant.now();
            runQuery(t, terminator);
            return Duration.between(start, Instant.now());
        } catch (final Exception e) {
            System.err.println("Error executing query '" + query + "': " + e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

    private static void runQuery(final Traversal t, final String terminator) {
        try {
            switch (terminator) {
                case ".toList()":
                    t.toList();
                    break;
                case ".next()":
                    t.next();
                    break;
                case ".iterate()":
                    t.iterate();
                    break;
                case ".toSet()":
                    t.toSet();
                    break;
                default:
                    throw new IllegalArgumentException("Unknown terminator: " + terminator);
            }
        } catch (final Exception e) {
            System.err.println("Error executing query: " + e.getMessage());
            e.printStackTrace();
            throw e; // Re-throw to allow caller to handle it
        }
    }

    private static HostInfo parseServerUrl(String serverUrl) {
        String host;
        int port = 8182; // Default port
        // Parse URL to extract host and port
        if (serverUrl.startsWith("ws://")) {
            serverUrl = serverUrl.substring(5);
        } else if (serverUrl.startsWith("wss://")) {
            serverUrl = serverUrl.substring(6);
        }

        if (serverUrl.contains(":")) {
            String[] parts = serverUrl.split(":");
            host = parts[0];
            try {
                port = Integer.parseInt(parts[1]);
            } catch (final NumberFormatException e) {
                throw new IllegalArgumentException("Invalid port in URL: " + parts[1]);
            }
        } else {
            host = serverUrl;
        }

        return new HostInfo(host, port);
    }

    private static Cluster getCluster(String serverUrl) {
        final HostInfo hostInfo = parseServerUrl(serverUrl);
        return Cluster.build()
                .addContactPoint(hostInfo.host)
                .port(hostInfo.port)
                .maxContentLength(65536)
                .create();
    }

    private static class HostInfo {
        String host;
        int port;

        HostInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }
    }
} 
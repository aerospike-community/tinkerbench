package com.aerospike;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

/*
Implements required interfaces to execute a workload AGS query.
 */
public abstract class QueryWorkloadProvider implements QueryRunnable {

    private final WorkloadProvider provider;
    private final AGSGraphTraversal agsGraphTraversal;
    private final LogSource logger = LogSource.getInstance();
    private final boolean isPrintResult;

    public QueryWorkloadProvider(final WorkloadProvider provider,
                                 final AGSGraphTraversal ags) {
        this.provider = provider;
        this.agsGraphTraversal = ags;
        this.isPrintResult = provider.getCliArgs().printResult;

        this.provider.setQuery(this);
    }

    /**
     * @return Logger instance
     */
    public LogSource getLogger() { return logger; }

    public OpenTelemetry getOpenTelemetry() { return provider.getOpenTelemetry(); }

    /**
     * @return true if this workload is a warmup...
     */
    @Override
    public boolean isWarmup() { return provider.isWarmup();}

    /*
    If true, this signals the result of the query should be displayed/logged.
     */
    @Override
    public boolean isPrintResult() { return isPrintResult; }

    /*
    Prints the result from a Query
     */
    public <T> void PrintResult(T result) {
        if (isPrintResult) {
            String value;
            if(result instanceof String) {
                value = (String) result;
            }
            else {
                value = result.toString();
            }

            System.out.println(value);
            logger.info(value);
        }
    }

    @Override
    public WorkloadTypes WorkloadType() {
        return WorkloadTypes.Query;
    }

    /**
     * @return the AGS Graph Traversal instance
     */
    @Override
    public GraphTraversalSource G() {
        return this.agsGraphTraversal.G();
    }

    /**
     * @return the AGS cluster instance
     */
    @Override
    public Cluster getCluster() {
        return this.agsGraphTraversal.getCluster();
    }

    public WorkloadProvider getProvider() { return provider; }

    @Override
    public QueryRunnable Start() {
        this.provider.Start();
        return this;
    }

    @Override
    public QueryRunnable PrintSummary() {
        this.provider.PrintSummary();
        return this;
    }

    @Override
    public QueryRunnable awaitTermination() {
        this.provider.awaitTermination();
        return this;
    }

    @Override
    public QueryRunnable Shutdown() {
        this.provider.Shutdown();
        return this;
    }

    @Override
    public String toString() {
        return String.format("Query[%s-%s]", Name(), WorkloadType());
    }
}

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
    private final String workloadName;
    private final IdManager idManager;

    public QueryWorkloadProvider(final WorkloadProvider provider,
                                 final AGSGraphTraversal ags,
                                 final IdManager idManager) {

        this.provider = provider;
        this.agsGraphTraversal = ags;
        this.idManager = idManager;
        this.workloadName = null;

        if(this.provider == null) {
            this.isPrintResult = false;
        } else {
            this.isPrintResult = this.provider.getCliArgs().printResult;
            this.provider.setQuery(this);
        }
    }

    public QueryWorkloadProvider(final WorkloadProvider provider,
                                 final AGSGraphTraversal ags,
                                 final IdManager idManager,
                                 final String workloadName) {
        this.workloadName = workloadName;
        this.provider = provider;
        this.agsGraphTraversal = ags;
        this.idManager = idManager;

        if(this.provider == null) {
            this.isPrintResult = false;
        } else {
            this.isPrintResult = this.provider.getCliArgs().printResult;
            this.provider.setQuery(this);
        }
    }

    @Override
    public String Name() {
        return this.workloadName;
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

    /*
    Returns a vertex Id from the IDManger or null
     */
    @Override
    public Object getVId() { return this.idManager == null ? null : this.idManager.getId(); }

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
        return String.format("Gremlin [%s-%s]", Name(), WorkloadType());
    }
}

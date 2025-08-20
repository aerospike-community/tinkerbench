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

    /*
    Returns a label used to obtain the ids in the sampling.
        If null, the default from the CLI is used.
     */
    @Override
    public String getSampleLabelId() { return null; }

    /*
    Returns the sampling size.
        If -1 to use the default value from the CLI.
        If zero, this will disable id sampling.
     */
    @Override
    public int getSampleSize() { return -1;};

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
    public void PrepareCompile() {}

    /**
     * Performs any required pre-process required for the query.
     * @return true to execute workload or false to abort execution
     */
    @Override
    public boolean preProcess() { return true; }

    /**
     * Performs any post-processing for the query
     */
    @Override
    public void postProcess() { }

    /*
   Called before the actual workload is executed.
   This is called within the scheduler and is NOT part of the workload measurement.
    */
    @Override
    public void preCall() {}

    /*
    Called after the actual workload is executed passing the value from the workload.
        The success param is true if the workload was recorded and exception will be not-null if an exciton occurred during workload execution.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    @Override
    public void postCall(Object value, Boolean success, Throwable error) {}


    @Override
    public String toString() {
        return String.format("Gremlin [%s-%s]", Name(), WorkloadType());
    }
}

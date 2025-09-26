# Tuning TinkerBench2 Queries per Second

![TinkerBench2 Tuning](./media/TinkerBench2%20Interface%20with%20Gremlin%20Mascot.png)

This section will review how to interpret TinkerBench2's output to optimize the scheduler and worker parameters. Tuning and optimizing of the [Tinkerpop](https://tinkerpop.apache.org/docs/current/reference/?utm_source=chatgpt.com) Client Driver, [JVM](https://javanexus.com/blog/boosting-java-performance-jdk-21-tips?utm_source=chatgpt.com), or [Gremline queries](https://github.com/tinkerpop/gremlin/wiki/Traversal-Optimization/2210259a510dca8183a69c76c8d11ccaf6e1c529?utm_source=chatgpt.com) is not within the scope of this document.

If TinkerBench2 is struggling to maintain or acheive the desired rate (QPS) adjustments to the number of schedulers and workers will typically resolve this issue without any need to adjust the JVM or Tinkerpop's client driver parameters.

## Understanding Output

TinkerBench2 provides a Query Depth metric. This metric shows how many pending queries are waiting for execution (queue depth).

This queue depth can be observed during execution and a summary report is produced upon workload completion.

TinkerBench2 produces three different outputs. They are:

- [Console](./understanding_output.md)
- [Log File](./understanding_output.md#logging)
- [Grafana Dashboard](./grafana_dashboard.md)

### Console and Log

#### Progression Bar (Only Console)

During a workload execution the progression bar provides an 1-second snapshot of the QPS and queue depth (pending label):

![Progression Bar](./media/ProgressionBarQueryDepth.png)

This information can be observed during execution to provides an overview of the current performance of TinkerBench2.

#### Query Depth Summary Report (Console and Log)

A Summary Query Depth report is produced upon completion of the workload.

Below is an example:

![Query Depth Summary Report](./media/QueryDepthSummary.png)

The Query Depth Report provides the following information:

- The average query depth, in this example is three.
- The maximum number of pending queries reached in the queue.
- The "occurrence rate" (e.g., 0.43%), indicates the percentage of the total maximum depth occurrences compared to the total queries executed in this workload.
  - For example, if the total number of queries executed by the workload is 6,000 and the "occurrence rate" is 0.43%. The number of times the maximum depth was hit was ~26 times.
- "25% depth" -- Depth was at or below 25% the total maximum depth occurrences (distribution of the pending queries).
- "50% depth" -- Depth was at or below 50% of the total maximum depth occurrences. This includes depth under 25%.
- "75% depth" -- Depth was at or below 75% of the total maximum depth occurrences. This includes depth under 25% and 50%.
- "90% depth" -- Depth was at or below 90% of the total maximum depth occurrences. This includes depth under 25%, 50%, and 75%.

These percentiles help to determine the efficiency of TinkerBench2's QPS maintenance.

##### Under QPS without Errors

If TinkerBench2 is not maintaining the QPS; try increasing the number of worker by the "occurrence  percentage" if this percentage is over 10%. If TinkerBench2 is using 10 workers and the rate is 10%, try increasing the workers to 11.

If the "occurrence  percentage" is less than 10%, try increasing the number of workers by 25%.

##### Maintaining QPS with Errors



 If the maximum depth's value is the same value for all the percentiles and the "occurrence rate" is over 10% with errors, this indicates a component (e.g., client platform, AGS, Aerospike DB, etc.) of the environment is underpowered. If this occurs, an evaulation of the enviorment is recommended.

Below is an example:






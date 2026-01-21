![A cartoon of a green alien with a black graduation cap AI-generated content may be incorrect.](docs/media/ASFremlinTB.png)

**TinkerBench** is a benchmarking tool designed for Graph databases based on [Apache TinkerPop](https://tinkerpop.apache.org/). It provides an efficient way to measure [Gremlin Query](https://docs.janusgraph.org/getting-started/gremlin/) performance in an easy and flexible manner.

# Overview

**TinkerBench** provides all the necessary performance metrics of the query. It provides latency, rate, count, and error information. This information is delivered in real time on the console or within a [Granfa dashboard](https://grafana.com/grafana/dashboards/). Information can be captured in a log file for post-processing or historical reference.

**TinkerBench** has two different ways to provide the Gremlin Query for analysis.

- As a query String -- Pass the actual query string by means of the command line for execution. The query is compiled and that result is used for measurement. Static or random vertex identifiers or property values can be used in the query.
- Java Jar – Provide a [Java Jar](https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jarGuide.html) file using the **TinkerBench** framework. With this method, you can pass the name of the custom query class by means of the command line for execution. This method provides an advanced way to handle complex configurations, complex queries, vertex/edge management, etc. The framework handles mundane things like measurements, error handling, logging, user interface, etc. The developer only needs to focus on the desired query behavior.

## Loading a Dataset

To be able to execute a query, a dataset must be loaded prior to execution. Loading can be achieved by means of the [Aerospike Bulk Loader](https://aerospike.com/docs/graph/develop/data-loading/overview/) or [importing a Gremlin](https://contextualise.dev/topics/view/15/gremlin) CSV format file.

## JVM Requirements

TinkerBench requires Java 21.

## Quick Start

These examples assume the [Air Routes dataset](https://aws.amazon.com/blogs/database/let-me-graph-that-for-you-part-1-air-routes/) has been loaded.

### Obtaining Help

Displays detail help on every argument.

```bash
java tinkerbench-*.jar --help
```

Displays version information for TinkerBench and TinkerPop modules

```bash
java tinkerbench-*.jar --version
```

Displays details on predefined queries.

```bash
java tinkerbench-*.jar list
```

### Aerospike Graph is running locally (Single Node)

This example runs the predefined query "AirRoutesQuery1" using the default workload duration of 15 minutes at a 100 Queries per Second rate. No warm up will be performed.

```bash
java tinkerbench-*.jar AirRoutesQuery1
```

This example runs the predefined query "AirRoutesQuery1" using the default workload duration of 15 minutes at a 100 Queries per Second rate. A warm up 1 minute warm up will be performed before the workload is executed.

```bash
java tinkerbench-*.jar AirRoutesQuery1 -wu 1M
```

This example runs the Gremlin string query using the default workload duration of 15 minutes at a 500 Queries per Second rate. A warm up 2 minute (120 seconds) warm up will be performed before the workload is executed.

```bash
java tinkerbench-*.jar "g.V(%s).out().limit(5).path().by(values('code','city').fold()).toList()" -wu 120 -qps 500
```

### Aerospike Graph is Running in a Cluster (Multiple Nodes)

This example runs the predefined query "AirRoutesQuery1" using the default workload duration of 15 minutes at a 100 Queries per Second rate. No warm up will be performed. Connect to three nodes in the Graph cluster.

```bash
java tinkerbench-*.jar AirRoutesQuery1 -n 10.0.0.1 -n 10.0.0.2 -n 10.0.0.3
```

# Topics

## [Understanding the Command Line Interface (CLI)](docs/understanding_command_line_interface.md)

## [Understanding Workload and Runtime Stages and QPS Sweeps](docs/understanding_workload_and_runtime_stages.md)

## [Understanding Errors](docs/understanding_errors.md)

## [Understanding Output](docs/understanding_output.md)

## [Writing Custom PreDefined Queries](docs/writing_predefined_queries.md)

## [Tuning TinkerBench](docs/tuning.md)

## [Grafana Dashboard](docs/grafana_dashboard.md)

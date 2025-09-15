![A cartoon of a green alien with a black graduation cap AI-generated content may be incorrect.](docs/media/ASFremlinTB.png)

**TinkerBench** is a benchmarking tool designed Graph databases based on [Apache TinkerPop](https://tinkerpop.apache.org/). It provides an efficient way to measure [Gremlin Query](https://docs.janusgraph.org/getting-started/gremlin/) performance in an easy and flexible manner.

# Overview

**TinkerBench** provides all the necessary performance metrics of the query. It provides latency, rate, count, and error information. This information is delivered in real time on the console or within a [Granfa dashboard](https://grafana.com/grafana/dashboards/). Information can be captured in a log file for post-processing or historical reference.

**TinkerBench** has two different ways to provide the Gremlin Query for analysis.

-   As a query String -- Pass the actual query string by means of the command line for execution. The query is compiled and that result is used for measurement. Static or random vertex identifiers can be used in the query.
-   Java Jar – Provide a [Java Jar](https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jarGuide.html) file using the **TinkerBench** framework. With this method, you can pass the name of the custom query class by means of the command line for execution. This method provides an advanced way to handle complex configurations, complex queries, vertex/edge management, etc. The framework handles mundane things like measurements, error handling, logging, user interface, etc. The developer only needs to focus on the desired query behavior.

## [Understanding the Command Line Interface (CLI)](docs/uderstanding_command_line_interface.md)

## [Understanding Workload and Runtime Stages](docs/understanding_workload_and_runtime_stages.md)

## [Understanding Errors](docs/understanding_errors.md)

## [Understanding Output](docs/understanding_output.md)

## [Grafana Dashboard](docs/grafana_dashboard.md)

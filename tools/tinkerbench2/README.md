# ![A cartoon of a green alien with a black graduation cap AI-generated content may be incorrect.](media/390439d4419f1c2c0b41ac34d2d68ff9.png)![A yellow rocket on a blue background AI-generated content may be incorrect.](media/9d5af70e414e86f57b0650aa3e3d6d67.jpg)TinkerBench 2

**TinkerBench** is a benchmarking tool designed for [Apache TinkerPop](https://tinkerpop.apache.org/) based graph databases. It provides an efficient way to measure [Gremlin Query](https://docs.janusgraph.org/getting-started/gremlin/) performance, making it easy for developers to evaluate their queries.

## Overview

**TinkerBench** provides all the necessary performance metrics of the query. It provides latency, rate, count, and error information. This information is delivered in real time on the console or within a [Granfa dashboard](https://grafana.com/grafana/dashboards/). Information can be also captured in file format for post-processing or historic reference.

**TinkerBench** has two different ways to supply the Gremlin Query for measurement.

-   Pass the actual query string by means of the command line for execution. The query is compiled and that result is used for measurement. Static or random vertex identifiers can be used in the query.
-   Create a [Java Jar](https://docs.oracle.com/javase/8/docs/technotes/guides/jar/jarGuide.html) file using the **TinkerBench** framework. With this method, you can pass the name of the custom query class by means of the command line for execution. This method provides an means of handling complex configurations, vertex/edge creation, etc. The framework handles measurements, error handling, logging, interface, etc. The developer only needs to focus on the desired query behavior.

# Details

## Understanding the Command Line Interface (CLI)

TinkerBench can be run by running the

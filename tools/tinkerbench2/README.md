This tool is designed to run and benchmark query workloads against the Aerospike Graph Service (AGS). It supports a simple extensible interface and integrates with Prometheus and Grafana for performance monitoring.

**🛠️ Getting Started**

**__Build__**

To compile the project, run the following from the root of the repository:
```
mvn clean install
```

**Usage**

The CLI includes full help documentation:

```
tinkerbench2 --help
```

To run a predefined workload with default values:

```
tinkerbench2 AirRouteQuery
```


**📦 Workloads**

The tool currently supports two workloads:

`QueryTest`: for basic testing

`AirRouteQuery`: queries on the Air Route graph database

You can add custom workloads by extending the `QueryWorkloadProvider` class.

**✏️ Defining a Custom Workload**

To create your own workload, simply extend QueryWorkloadProvider. No other code changes are required.
After adding your class:

```sh
mvn clean install
tinkerbench2 MyWorkloadQuery
```

Here's an example:

```java
package com.aerospike;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class MyWorkloadQuery extends QueryWorkloadProvider {

    public MyWorkloadQuery (final WorkloadProvider provider,
                          final AGSGraphTraversal ags) {
        super(provider, ags);
    }

    /**
     * @return thw Query name
     */
    @Override
    public String Name() {
        return "MyWorkload";
    }

    /**
     * @return The query description
     */
    @Override
    public String getDescription() {
        return "My Workload   Graph Traversal";
    }

    /**
     * Performs any required pre-process required for the workload.
     * @return true to execute workload or false to abort execution
     */
    @Override
    public boolean preProcess() {
        return true;
    }

    /**
     * Performs any post-processing for the workload
     */
    @Override
    public void postProcess() {

    }

    /**
     * @return true to indicate that the workload was successful and should be recorded.
     *  False to indicate that the workload should not be recorded. This also occurs for any exceptions
     * @throws Exception
     */
    @Override
    public Boolean call() throws Exception {
        G().V(3).out().limit(5).path().by(values("code","city").fold()).toList();
        return true;
    }
}
```

****📈 Monitoring with Prometheus****

To enable Prometheus metrics, use the -prom flag using the proper port (run --help for info).

```
tinkerbench2 MyWorkloadQuery -prom
```
This currently provides the best interface. The initial Grafana dashboard can be found at here.
To visualize the metrics, you can use the Grafana dashboard provided here .
 ```tools/asgworkload/Aerospike Workload.json```

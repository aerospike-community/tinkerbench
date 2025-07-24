This tool is designed to run and benchmark query workloads against the Aerospike Graph Service (AGS). It supports a simple extensible interface and integrates with Prometheus and Grafana for performance monitoring.

🛠️ Getting Started
Build
To compile the project, run the following from the root of the repository:
```
mvn clean install
```

Usage
The CLI includes full help documentation:

```
agsworkload --help
```

To run a predefined workload with default values:

```
agsworkload AirRouteQuery
```


📦 Workloads

The tool currently supports two workloads:

`QueryTest`: for basic testing

`AirRouteQuery`: queries on the Air Route graph database

You can add custom workloads by extending the `QueryWorkloadProvider` class.

✏️ Defining a Custom Workload
To create your own workload, simply extend QueryWorkloadProvider. No other code changes are required.
After adding your class:

```sh
mvn clean install
agsworkload MyWorkloadQuery
```

Here's an example:

```java
package com.aerospike;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class MyWorkloadQuery extends QueryWorkloadProvider {

    public MyWorkloadQuery(final WorkloadProvider provider,
                           final AGSGraphTraversal ags) {
        super(provider, ags);
    }

    @Override
    public String Name() {
        return "MyWorkload";
    }

    @Override
    public String getDescription() {
        return "My Workload Graph Traversal";
    }

    @Override
    public boolean preProcess() {
        return true;
    }

    @Override
    public void postProcess() {
        // Optional cleanup logic
    }

    @Override
    public Boolean call() throws Exception {
        G().V(3).out().limit(5).path().by(values("code", "city").fold()).toList();
        return true;
    }
}
```

📈 Monitoring with Prometheus
To enable Prometheus metrics, use the -prom flag using the proper port (run --help for info).

```
agsworkload MyWorkloadQuery -prom
```
This currently provides the best interface. The initial Grafana dashboard can be found at here.
To visualize the metrics, you can use Grafana.
 ```tools/asgworkload/Aerospike Workload.json```

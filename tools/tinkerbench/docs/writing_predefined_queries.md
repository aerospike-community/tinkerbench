# Writing a Custom Predefined Gremlin Query

![Predefined Query](media/Gremlin%20Query%20Diagram%20for%20Tinkerbench.png)

TinkerBench allows the extension of existing and newly defined predefined Gremlin queries.

This feature allows developers to wire very complex Gremlin queries that cannot be represented as a Gremlin query string.

This also allows extension/enhancement of the device id management.

## Predefined Gremlin Query

A predefined query is a [Gremlin Query](https://tinkerpop.apache.org/gremlin.html) using the Java Gremlin driver's API and embeeded into the TinkerBench framework.

## TinkerBench Framework

The framework provides all the connection management, performance measurement, reporting, exception handling, logging, and Grafana management. The developer only needs to focus on the requirements of the Gremlin query.

## How to enable an Predefined Query Jar

Using the Java runtime command-line system property option providing the location of the Java Jar file. Multiple Jar files can be provided by separating each file by a semicolon. The location of the Jar file can also be defined in the Java class path. Below is an example:

```bash
java -DTBQryJar=.\TB2PreDefinedQuery.jar TinkerBench-2.0.14-jar-with-dependencies.jar
```

## Java Project Setup

Create a Java 21 project and reference the TinkerBench's Jar file with dependiencs. This library will provide all the required libraries needed to execute the Gremlin query.

In your newly created project, create a class using the following package and imports:

```java
package com.aerospike.predefined;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManager;
import com.aerospike.QueryWorkloadProvider;
import com.aerospike.WorkloadProvider;
```

Once completed the new class should extend from `QueryWorkloadProvider`.

The new claas requires the following methods:

- A constructor that takes three arguments. These arguments are required o be passed to `super`. The argument types are:
  - WorkloadProvider
  - AGSGraphTraversal
  - IdManager
- Create a `Name` method that returns a sting value. This will be the name of the predefined query and typically should match the name of the class.
- Create a `getDescription` method that returns a string that describes the predefined query. This should return a string representation of the Gremlin query.
- Create a `call` method that returns a pair of values where the first element is a boolean and the second an object. This method is called to execute the Gremlin query. This method should only define the query and perform no other operations since this method is used to record the query's performance.
  - Typically the first returned element shold be true. This element is used to determine if the resulting latency should be recorded or not. True indicates, record query's latency. False indicates, to not to record latency.
  - Typiclly the second returned element should be null. This element is passed to the `postCall` optionally defined method (see below for more information).

## Simple Example

```java
package com.aerospike.predefined;

import com.aerospike.AGSGraphTraversal;
import com.aerospike.IdManager;
import com.aerospike.QueryWorkloadProvider;
import com.aerospike.WorkloadProvider;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.javatuples.Pair;

import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.values;

public class AirRoutesQuery2 extends QueryWorkloadProvider {

    public AirRoutesQuery2(final WorkloadProvider provider,
                           final AGSGraphTraversal ags,
                           final IdManager idManager) {
        super(provider, ags, idManager);
    }

    /**
     * @return thw Query name
     */
    @Override
    public String Name() {
        return "AirRoutesQuery2";
    }

    /**
     * @return The query description
     */
    @Override
    public String getDescription() {
        return "Air Routes Graph Traversal Query 2:\n\tG().V( getVId() ).out().limit(5).path().by(values(\"code\",\"city\").fold()).toList();";
    }

    /**
     * @return true to indicate that the workload was successful and should be recorded.
     *  False to indicate that the workload should not be recorded.
     *  If an exception occurs the exception is only recorded. If an InterruptedException occurs, this is treated like a false return.
     *  The second item in the return is null since there is no post-call processing required.
     */
    @Override
    public Pair<Boolean,Object> call() throws Exception {
        final List<Path> result =  G().V( getVId() )
                .out()
                .limit(5)
                .path()
                .by(values("code","city")
                        .fold())
                .toList();

        //The below code is not required since the result can be ignored.
        if(isPrintResult())
            result.forEach(this::PrintResult);

        return new Pair<>(true,null);
    }
}
```

## Advance Topics

### Per/Post Methods for Initialization and Cleanup

Below is a list of methods that can be overridden to perform pre-query (e.g., query setup) and post-query (e.g., cleanup) operations.

- `preProcess` This method returns a boolean value. This method is called **only once** before the predeinfed query is called. It can be used to setup any "global" initialization. It should return true to indicate that the predfined query should be scheduled for execution.
- `postProcess` This method is called when query execution is completed. It is only called once. This is used to perform any "global" cleanup.
- `preCall` This method is called before each query execution.
- `postCall` This method takes three arguments. It is called after each query is executed. Below are the arguments:
  - The first argument is an object. This value is the second value returned from the `call` method. This value can be null.
  - The second argument is an boolean. This value indicates if the `call` method completed successfully (true). A value of false can indicate the query waas not executed.
  - The third argument is an exception type. If not null, this would be the error that was thrown during execution of the `call` method. Note, the framework has already recorded the exception.

### Helper Methods

Below are a subsrt of methods that can aid in development:

- `getLogger` This method returns the logging instance.
- `getOpenTelemetry` This method returns the [Open Telemetry](https://opentelemetry.io/docs/languages/java/) instance used to update [Prometheus](https://opentelemetry.io/blog/2024/prom-and-otel/).
- `getVId` This returns a random vertex id from the [Id Manager](./vertex_id_manager.md) or null, if this feature is disabled.
- `isWarmup` This method returns true to indicate the upcoming query execution is for a warm up phase. False to indicate actual workload run.
- `isPrintResult` Returns true to indicate that the result of the query should be printed to the console or log. See `PrintResult` method.
- `PrintResult` This method will print the argument to console and log.
- `WorkloadType` This method returns the work load type (i.e., Query, GremlinString, Test).
- `G` This method returns a [GraphTraversalSource](https://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/process/traversal/dsl/graph/GraphTraversalSource.html) instance.
- `getCluster` This method returns the [TinkerPop Gremlin Driver's Cluster](https://tinkerpop.apache.org/javadocs/current/core/org/apache/tinkerpop/gremlin/driver/Cluster.html) instance.
- `getProvider` This method returns an TinkerBench's WorkloadProvider instance. This class is responsible for scheduling and executing the query workload. It provides access to many of the internal state, [HDRHistogram](https://github.com/HdrHistogram/HdrHistogram) instances, etc.

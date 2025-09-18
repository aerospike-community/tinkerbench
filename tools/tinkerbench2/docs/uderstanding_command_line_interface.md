# Understanding the Command Line Interface (CLI)

To obtain the command-line arguments with description and examples run TinkerBench with the “—help” flag. If an argument is not valid, an error message is displayed providing feedback so that a proper value is supplied.

| ![image](media/gremlin-apache.png) | Arguments can be passed in as a [Java property file](https://localizely.com/java-properties-file/) instead of the command line. If a property file is provided, you can override the file’s property values by means of the command line. For more information, see [Using a Property File section](#using-a-cli-property-file). |
|------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|

Below are the arguments and description:

-   QueryNameOrGremlinString (String, Required) – This is a required argument that can be located as the first or last argument in the command line. There are three value forms this argument can take. They are:
    -   A Gremlin query string. This string will be compiled and executed for analysis. **Examples**:
        -   `g.V(3).out().limit(5).path().by(values('code','city').fold()).tolist()`
        -   `g.V(%s).out().limit(5).path().by(values('code','city').fold()).tolist()` Where ‘%s’ will be substituted with a random vertex id from the vertex manager. For more information see \<\*\*vertex manager\>.
    -   A Predefined Query. A query defined using the TinkerBench framework. For more information see [Writing Predefined/Advance Queries](./writing_predefined_queries.md).
    -   The keyword ‘List’. If provided all other arguments are ignored. This will list all the predefined queries found in the [Java class path](https://en.wikipedia.org/wiki/Classpath). For more information see [Writing Predefined/Advance Queries](./writing_predefined_queries.md).
-   \--host, -n, -a (String, Default localhost) – One or more graph nodes’ IP address or host name. To provide multiple nodes, each node must be paired with this argument. **Examples**:
    -   `–n myGraphNodeName`
    -   `--host 10.1.1.1123`
    -   `-n 10.1.1.1123 -n 10.1.1.1124 -n 10.1.1.1125`
    -   `-n 10.1.1.1123,10.1.1.1124,10.1.1.1125`
        Example of providing multiple graph node addresses
-   \--port (Integer, Default 8182) – Graph node’s connection port number.
-   \--QueriesPerSec, -qps, -q (Integer, Default 100) -- The targeted number of queries per seconds. TinkerBench will try to achieve and maintain this target for the query duration based on the scheduler and worker arguments. See \<\*\*tuning\> for additional information.
-   \--duration, -d (Time, Default 15 minutes) -- The time duration the query is executed for analysis. This would be the main workload for complete analysis. This duration should be long enough for TinkerBench to achieve its’ targeted query rate. The value can take multiple forms. They are:
    -   [ISO 8601](https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html) format **Example**: PT1H2M3.5S – one hour, 2 minutes and 3.5 seconds
    -   A number (integer) of seconds **Example**: 45 – 45 seconds
    -   xHour(s)\|Hr(s)\|HyMinute(s)\|Min(s)\|MzSecond(s)\|Sec(s)\|S – Where x, y, and z are integers. The unit of time (H, M, S, Hour, etc.) are case insensitive. **Example**:
        -   1h30s – one hour and 30 seconds
        -   2hrs45seconds – two hours and 45 seconds
        -   3hours5mins30s – three hours, 5 minutes, and 30 seconds
-   \--WarmupDuration, -wm, -wu (Time, Default disabled) – The query “warmup” duration. The warmup is run using the same graph connection that will be used by the main query workload. The warmup helps the graph database to optimize the query and aids TinkerBench in achieving the targeted rate during the main workload analysis. This takes the same value format as the “duration” argument above. A value of zero (0) will disabled the warmup which is the default.
-   \--schedulers, -s (Integer, Default depends on cores) – Schedulers are used to manage workers to control the query rate. The default number of schedules is based on the quarter of the number of cores of the machine TinkerBench is currently executing on. A value of -1 will indicate to use the default value. For more information, see the \<\*\*tuning\> section. **Example**: 20 core machine -\> there will be 4 schedulers.
-   \--workers, -w (Integer, Default depends on cores) -- The number of workers per scheduler. A worker is responsible for executing a single query instance and collecting data from that instance for analysis. The default number of workers is based on half of the number of cores of the machine TinkerBench is currently executing on. A value of -1 will indicate to use the default value. For more information, see the \<\*\*tuning\> section. **Example**: 20 core machines -\> there will be 10 workers per scheduler (total of 40 workers over 4 schedulers).
-   \--IdSampleSize, -sample (Integer, Default 500,000) – The number of vertex ids that will be retrieved from the database that will be used as a vertex id by the query, if required. If the query doesn’t use a random vertex id, this feature is disabled. See argument “*—IdSampleLabel*” for additional information. For more information, see \<\*\*vertex manager\>.
-   \--IdSampleLabel, -label (String, Default is None) – If provided, this is a label that is used in retrieving the vertex ids from the database. These ids, if required by the query, are used by the query as a random vertex id to the query. If the See argument *“—IdSampleSize*” for additional information. For more information, see \<\*\*vertex manager\>.
-   \--Prometheus, -prom (Flag) – If provided, enables the [Prometheus](https://prometheus.io/) exporter which provides near real-time metrics of the running TinkerBench application in TinkerBench [Grafana](https://grafana.com/grafana/dashboards/) dashboard. For more information, see [Grafana Dashboard](./grafana_dashboard.md) section.
-   \--HdrHistFmt, -hg -- If provided, the summary console output upon exit of the TinkerBench application will provide an [HdrHistogram](https://github.com/HdrHistogram) Latency table. This table can be used by the [HdrHistogram plotter](https://hdrhistogram.github.io/HdrHistogram/plotFiles.html). If not provided a “Summary latency” is provided. For more information, see the [Output](./understanding_output.md) section. **Note**: The HdrHistogram table is always provided in the log file, if logging is enabled.
-   \--Errors, -e (Integer, Default 150) – The total number of error occurrences that will cause TinkerBench to shutdown query analysis and display the console summary.
-   \--version, -V – Prints the TinkerBench and Gremlin client version information.

### Advance or Rarely Used Arguments

-   \-background (Flag) – This flag will indicate that TinkerBench2 will be running as a “background” process. In this mode, certain console output (e.g., progression bar) is suppressed.
-   \--clusterBuildConfigFile, -bf (File Path) – [Gremlin cluster builder configuration](https://www.gremlin.com/docs/getting-started-agent-configuration) file.
-   \--clusterBuild, -b (Key/Value Pair) – One or more [Cluster Builder](https://tinkerpop.apache.org/javadocs/current/full/org/apache/tinkerpop/gremlin/driver/Cluster.Builder.html) options as a key/value pair (OptionName=OptionValue). Examples:
    -   “-b maxConnectionPoolSize=10 -b maxInProcessPerConnection=100”
        This will update the maximum size of the ConnectionPool and the maximum number of in-flight requests for a Connection.
-   \--gremlin, -g (Key/Value Pair) – One or more Gremlin traversal source configuration options (i.e., [step modulator](https://tinkerpop.apache.org/docs/current/tutorials/gremlins-anatomy/)) as a key/value pair (OptionName=OptionValue). **Example**:
    -   `“-g evaluationTimeout=30000 -g paging=2”`
        This will update the traversal with an “evaluation time” of 30 seconds and a “paging” value of 2
-   \--shutdown, -sd (Time, Default 15 seconds) -- Additional time to wait after workload completion (normal or aborted) to allow for proper cleanup. Typically, the default provides enough time for cleanup.
-   \--prometheusPort (Integer, Default 19090) – The endpoint port used by [Prometheus](https://prometheus.io/docs/prometheus/latest/configuration/configuration/) to obtain the metrics used by the Granfa dashboard.
-   \--CloseWait (Time, Default 5 seconds) – The wait interval used upon application exit to ensure Prometheus has obtained all the required information. This value should match or exceed the ['scrape interval](https://prometheus.io/docs/prometheus/latest/configuration/configuration/)' in the Prometheus ymal file. This argument is ignored, if Prometheus is disabled (*-no-prom*). **Note**: It is recommended that the scrape interval for the Prometheus TinkerBench job be set to 5 seconds.
-   \--result, -r (Flag) -- Enables the results of **every** Gremlin query execution to be displayed and logged. Should only be used for debugging purposes.
-   \-debug (Flag) -- Enables application debugging tracing and “DEBUG” logging. Should only be used for debugging purposes.

## Using a CLI Property File

Command line arguments can be passed to TinkerBench2 by means of a [Java property file](https://localizely.com/java-properties-file/) instead of the command line. Even if a property file is provided, you can override that value by means of the command line.

Below is an example of a property file that defines the following options:

-   Uses the predefined query “AirRoutesQuery1”
-   Connect to AGS hosts 10.1.1.1123, 10.1.1.1124, and 10.1.1.1125
-   The workload runs for 1 minute with a 1-minute warmup
-   The workload should reach and maintain 200 queries per second
-   Enable Prometheus
-   Define two Gremlin traversal source configuration options (i.e., evaluation timeout and paging)

```
# Comment
QueryNameOrGremlinString=AirRoutesQuery1
host=10.1.1.1123, 10.1.1.1124, 10.1.1.1125
duration=1M
WarmupDuration=1M
QueriesPerSec=200
prometheus=true
gremlin=evaluationTimeout=30000, paging=2
```

The property key must be the long name of the option (e.g., “—host”, “—duration”, etc.). Any option that can be provided multiple times with different values (e.g., “—host”) are provided as a list of values separated by a comma. Flags (e.g., “—prometheus”) will take a Boolean value (true or false). Comments should start with a “\#”.

### Enabling the CLI Property File

You can use the Java runtime command-line system property option to define the location of the property file or place a file named “.tinkerbench2.properties” in user’s home directory.

Below is an example using the Java runtime command-line method:

```
java -Dpicocli.defaults.tinkerbench2.path=.\mytbpropfile.properties tinkerbench2-2.0.14-jar-with-dependencies.jar
```
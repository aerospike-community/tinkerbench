![Workload](media/gremlin-logo-Running.png)
# Understanding Workload and Runtime Stages

TikerBench2 has two workload run types. They are:

-   Warmup – Conducted if the warmup duration (`--WarmupDuration`) is defined. This executes the query like “workload” for the purpose of “warming up” the database. Meeting the targeted QPS rate may or may not occur. Summary results are captured and reported. This run is not required.
-   Workload – This preforms the “**main”** workload and strides to make and maintain the QPS rate for the duration of the run. Results are captured and reported based on the configuration provided.

When TinkerBench executes it goes through the following stages:

1.  Initialization Performs checks of the configurations, prepares and connections to the database, etc.
2.  Vertex Id retrieval, if required Obtains vertex ids, if required by the query. These ids are randomly selected for each execution of the query.
3.  Preparing and Completion of the Gremlin Query String, if required If a Gremlin query string is provided, the string is prepared and compiled into bytecode for repeated executing during workload run phase.
4.  Workload Run Phase (i.e., run Warmup and/or Main Workload) For each workload run type the following phases are executed:
    1.  Preprocessing The workload scheduler is prepared to execute the query
    2.  Start
    3.  Iteration This phase is run until the duration is obtains. The scheduler adjusts the workers to obtain the QPS rate based on the provided configuration.
    4.  Postprocessing This phase performs result collection, cleanup, etc.
    5.  Shutdown
5.  TinkerBench Cleanup
6.  Report Results This stage will display the results based on the configuration provided.

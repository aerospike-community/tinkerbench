
## Simple Benchmark


### Steps to use the Simple Benchmark
- Build  `mvn clean install` to build the project
- Configure   `./conf/simple.properties` and ensure all the parameters are correct
  - **Most importantly** Ensure that  graph.server.host and graph.server.port refer to the accessible IP address and port of the AGS instance.
- Run  `./scripts/run_simple.sh`

### What Simple Benchmark does 
The Simple benchmark does the following setup / benchmark process.

### Setup
1. Clears the graph (g.V().drop())
2. Writes `benchmark.seedSize` number of vertices with ids `1-benchmark.seedSize`

### Benchmark
- Get or create vertices with a random ID between `1-benchmark.seedSize*benchmark.seedSize.multiplier`
- Create edges between randomly chosen vertices
- Pick a random vertex and do a 3 hop traversal



## Simple Benchmark

The Simple benchmark does the following setup / benchmark process.

### Setup
1. Clears the graph (g.V().drop())
2. Writes `benchmark.seedSize` number of vertices with ids `1-benchmark.seedSize`

### Benchmark
- Get or create vertices with a random ID between `1-benchmark.seedSize*benchmark.seedSize.multiplier`
- Create edges between randomly chosen vertices
- Pick a random vertex and do a 3 hop traversal

### Steps to use the Simple Benchmark
- Run `./scripts/build-docker.sh` to build the docker image
- Edit `./scripts/run_docker.sh` and ensure all the parameters are correct
    - Most importantly the graph.server.host and graph.server.port. These refer to the accessible IP address and port of the AGS instance.
- Run the script. `./scripts/run_docker.sh`

# Tinkerbench

Tinkerbench is tool for running and managing benchmarks on TinkerPop based graph databases.  

Currently supported benchmarks:
  - Identity Graph
  - Load benchmark

## Load Benchmark

The load benchmark is a simple benchmark that does the following:

### Setup
- Clear the graph
- Load a graph with seed size

### Benchmark
- Run many mergeV's with seed size * seed multiplier
- Run many edge inserts that attach vertices

## Identity Benchmark

The identity benchmark is composed of two components, `BenchmarkStitching` and `BenchmarkShortRead`.

### Steps to use the Identity Benchmark

1. Before loading the graph, create secondary indexes on the required properties. To do this, run ./scripts/run_create_indexes.sh
2. Using `graph-synth` (see https://github.com/aerospike/graph-synth/tree/benchmark-schema-compat), load the graph with the identity schema

   Note you will need to configure the scale-factor. This may take some trial and error to get right. For reference, scale factor of 10000 creates a graph with 10k GoldenEntities 

   Here is an example of how to run graph-synth for the identity schema:

   `mvn clean install -DskipTests`

   `java -jar ./graph-synth/target/GraphSynth-1.1.0-SNAPSHOT.jar   --input-uri=file:$(pwd)/conf/schema/benchmark2024.yaml   --output-uri=ws://localhost:8182/g --scale-factor=10 --clear`
3. Run the stitching, see `./scripts/run_stitch.sh` and `./conf/stitch.properties` to run and configure the stitching benchmark
4. Run the short read, see `./scripts/run_shortread.sh` and `./conf/shortread.properties` to run and configure the short read benchmark

### Requirements to run the Identity Benchmark

- A TinkerPop based graph database that the benchmark can connect to
- Java 17 CLI installed

### Notes

- Getting the correct stitching amount may be a little tricky since JMH does not run in absolute iterations, but rather in time


### Next steps

Objective: Gain intuition on how big of scale factor / how long to run stitching for proper results

1. Run graph-synth with scale factor 1 million
2. Adjust stitching config to 10 minute run (this is done in the conf/stitch.properties file) and run ./scripts/run_stitch.sh
3. Run ./scripts/run_summary.sh to get the number of stitched vertices. Extrapolate how much more time we need to run this for the number of stitched vertices to be ~1/2 the number of GoldenEntities
4. Check Aerospike and see how much data we are using. From here extrapolate how much bigger our scale factor needs to be to read 100 GB, 1 TB, and 10 TB
5. From here we can run the ./scripts/run_shortread.sh just as a smoke test and initial results on a small scale.
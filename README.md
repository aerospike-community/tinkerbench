# Tinkerbench

Tinkerbench is tool for running and managing benchmarks on TinkerPop based graph databases.

Currently, there is one support benchmark, which is the identity benchmark.

## Identity Benchmark

The identity benchmark is composed of two components, `BenchmarkStitching` and `BenchmarkShortRead`.

### Steps to use the Identity Benchmark

1. Using `graph-synth` (see https://github.com/aerospike/graph-synth/tree/benchmark-schema-compat), load the graph with the identity schema

   Here is an example of how to do this for a data on localhost. Note you will need to configure the scale-factor. This may take some trial and error to get right.

   For reference, scale factor of 10000 creates a graph with 

   `mvn clean install -DskipTests`

   `java -jar target/GraphSynth-1.1.0-SNAPSHOT.jar   --input-uri=file:$(pwd)/conf/schema/benchmark2024.yaml   --output-uri=ws://localhost:8182/g --scale-factor=10 --clear`
2. Run the stitching, see `./scripts/run-stitching.sh` and `./conf/stitch.properties` to run and configure the stitching benchmark
3. Run the short read, see `./scripts/run-shortread.sh` and `./conf/shortread.properties` to run and configure the short read benchmark

### Requirements to run the Identity Benchmark

- A TinkerPop based graph database that the benchmark can connect to
- Java 17 CLI installed

### Notes

- Getting the correct stitching amount may be a little tricky since JMH does not run in absolute iterations, but rather in time

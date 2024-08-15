# Tinkerbench

Tinkerbench is tool for running and managing benchmarks on TinkerPop based graph databases.
It is built on the JMH benchmarking framework. https://github.com/openjdk/jmh and customised for running benchmarks on any tinkerpop based Graph database.

Currently supported benchmarks are:
- Simple benchmark
- Identity Graph benchmark

## Simple Benchmark

Requirements: To run this benchmark you must have Aerospike Graph Service running and connected to Aerospike.
If you already have AGS setup and connected to Aerospike database proceed with the steps below. If not, you can use Aerolab to quickly setup Aerospike Graph.
Instructions for using Aerolab to start up Aerospike Graph Service and Aerospike are at the bottom of the page.

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

# Starting Aerospike Graph Service and Aerospike with Aerolab

Aerolab can be downloaded from https://github.com/aerospike/aerolab.

## GCP

### Example of setting up Aerospike in GCP

```
aerolab cluster create -n demo -c 2 --instance n2-standard-8 --zone us-west4-a --disk=pd-ssd:300 --disk=local-ssd@1 --disk=pd-ssd:380@1 --gcp-expire 200h
```

### Example of setting up Aerospike Graph Service in GCP

```
aerolab client create graph -n graph --count 1 --cluster-name demo --namespace test --zone us-west4-a --instance c2-standard-8 --gcp-expire 200h
```

## AWS

### Example of setting up Aerospike in AWS

```
aerolab cluster create -n demo -c 3 -I i4i.2xlarge --aws-expire 120h
```

### Example of setting up Aerospike Graph Service in AWS

```
aerolab client create v graph -n graph --count 1 --cluster-name aim-cluster --namespace test -I i4i.2xlarge
```

## Destroying a cluster

```
aerolab cluster destroy -n <name>
```

## Common tasks after setup

### SSH into a node

```
ssh -i aerolab-graph_us-west-1
```

### Verifying Aerospike Graph Service is running

```
sudo docker ps -a
```

### Launch Gremlin Console using Aerolab

```
aerolab client attach -n graph -- docker run -it --rm tinkerpop/gremlin-console
```

### Access Terminal on Aerospike Graph Service

```
aerolab attach client -n graph
```


## Identity Benchmark

The identity benchmark has two benchmark workloads, `BenchmarkShortWrite` and `BenchmarkShortRead`.

### Requirements to run the Identity Benchmark

- A TinkerPop based graph database that the benchmark can connect to
- Java 17 CLI installed

### Instructions for Running the Identity Benchmark 
1. **Setup Indexes**
Before loading the graph data, you need to create secondary indexes on the required properties. To create these indexes, run the following script:
```bash
./scripts/run_create_indexes.sh
```
2. **Load the Data**
	You will need Graph Synth, available at [GraphSynth GitHub](https://github.com/aerospike/graph-synth/tree/main) Repository, to generate the required dataset.
   - Refer to the Graph Synth repository for detailed instructions and additional options.
   - Ensure you have access to the released JAR file from the Graph Synth repository before running the data generation command.

To stream data directly into Aerospike Graph, you can use the following command:
```bash
java -jar ./graph-synth/target/GraphSynth-1.1.0-SNAPSHOT.jar --input-uri=file:$(pwd)/conf/schema/benchmark2024.yaml --output-uri=ws://localhost:8182/g --scale-factor=100000 --clear
```
**Note**: If Graph Synth is running on a different system, replace localhost with the AGS endpoint.

3. **Running the Benchmark**

   a. Run the following command to clean and build the project:
   ```bash   
      mvn clean install
   ```
   b. Configure Benchmark Properties
   Edit ```./conf/shortread.properties``` or ```./conf/shortwrite.properties``` to ensure all parameters are correctly set. Pay special attention to the following properties:
      ```graph.server.host```
      ```graph.server.port```
   These should point to the accessible IP address and port of your AGS instance.

   c. Run the benchmark 
   Execute the following script to run the benchmark:
   For Short Reads
   ```bash   
   ./scripts/run_shortreads.sh
   ```
   For Short Writes
   ```bash   
   ./scripts/run_shortwrites.sh
   ```   






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
Bulk load the identity benchmark dataset using the [appropriate method](https://aerospike.com/docs/graph/data-loading). 
Datasets for multiple scale factors are available [here](https://console.cloud.google.com/storage/browser/identity-benchmark)

| Dataset Name	| Bulk Load Method |
| ------------ 	| ---------------- |
| SF1000  	| Standalone	   |
| SF10000 	| Standalone	   |
| SF100000   	| Standalone	   |
| SF1M   	| Standalone	   |
| SF10M   	| Distributed	   |
| SF100M   	| Distributed	   |
| SF1B   	| Distributed	   |


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





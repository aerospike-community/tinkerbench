# Tinkerbench

**Tinkerbench** is a benchmark harness tool designed for [Apache TinkerPop](https://tinkerpop.apache.org/) based graph databases. It provides an efficient way to build, run, and analyze benchmarks, making it easy for developers to evaluate and optimize the performance of their gremlin queries.

It is a framework to help you:

* **Construct your own Benchmarks**: Create benchmarks to mimic your application workload and test the performance of your graph database.

* **Execute your Benchmarks**: Configure and run your benchmarks to mimic your application workloads. Analyze performance data and optimize your queries and configurations. 


Although Tinkerbench is designed to be flexible and extensible, it is currently tested only with Aerospike Graph as the graph database. As such, the instructions and examples are specific to Aerospike Graph.

### **Getting Started**

#### **Prerequisites**
* A TinkerPop based graph database for the benchmark to connect to. 
To use Tinkerpop with Aerospike Graph, you need to have Aerospike Graph Service (AGS) setup and running. If you already have AGS setup and connected to Aerospike database proceed with the steps below. If not you can:
1. Use [Aerospike Graph Quickstart](https://github.com/aerospike/aerospike-graph) to spin up AGS and Aerospike database
2. Use Aerolab to quickly [setup Aerospike Graph](setup_aerospike_graph.md).
* Java 17
* Maven

#### **Installation**
1. Clone the repository
   ```bash
    git clone https://github.com/aerospike-community/tinkerbench.git
    cd tinkerbench

### **Running Benchmarks**
There are two benchmarks available in Tinkerbench out of the box. 
- [Simple benchmark](simple_bm.md)
- [Identity Graph benchmark](identity_bm.md)

### **Contributing**

Contributions are welcome and encouraged! If you'd like to contribute to Tinkerbench, please submit a pull request or file an issue on the repository.
Please ensure that no proprietary information is included in your contributions.

### **License**

Tinkerbench is released under the MIT license. See [LICENSE](LICENSE) for more information.

#!/usr/bin/env bash
### Configs
# graph.server.host -                       ip for graph
# graph.server.port -                       port for graph
# graph.client.ssl -                        whether to use ssl
# graph.client.maxConnectionPoolSize -      maximum client connections to use. 
# graph.client.minConnectionPoolSize -      minimum client connections to use. 
# graph.client.maxInProcessPerConnection -  max number of requests to send per connection
# graph.client.maxSimultaneousUsagePerConnection - max simultaneous usage of a connection
# graph.client.minSimultaneousUsagePerCOnnection - min simultaneous usage of a connection
# benchmark.measurementForks -              how many forks to use - 1 is okay generally
# benchmark.measurementIterations -         how many times to run the benchmark of measurementTime
# benchmark.measurementTime -               how long to run a single benchmark
# benchmark.measurementTimeout -            how long to wait before timing out a single benchmark (should be 1 larger than measurement time)
# benchmark.measurementThreads -            how many threads to run benchmark on
# benchmark.seedSize -                      how many different ids to start the graph with (initialize)
# benchmark.seedSize.multiplier -           multiplier of the seed size for the number of ids to be used in the benchmark.
# benchmark.mode -                          Allowable values all, throughput, average, sample. Refer to https://javadoc.io/doc/org.openjdk.jmh/jmh-core/latest/org/openjdk/jmh/annotations/Mode.html for details on the mode#
sudo docker run -t -i \
    -e graph.server.host=172.170.1 \
    -e graph.server.port=8182 \
    -e graph.client.maxConnectionPoolSize=4 \
    -e graph.client.minConnectionPoolSize=4 \
    -e graph.client.maxInProcessPerConnection=8 \
    -e graph.client.maxSimultaneousUsagePerConnection=8 \
    -e graph.client.minSimultaneousUsagePerConnection=8 \
    -e benchmark.measurementForks=1 \
    -e benchmark.measurementTime=5 \
    -e benchmark.measurementTimeout=15 \
    -e benchmark.measurementIterations=4 \
    -e benchmark.measurementThreads=16 \
    -e benchmark.seedSize=50000 \
    -e benchmark.seedSize.multiplier=4 \
    -e benchmark.mode=throughput \
tinkerbench

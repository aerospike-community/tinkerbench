package com.aerospike.tinkerbench;

import org.openjdk.jmh.runner.RunnerException;

import java.io.IOException;

import static com.aerospike.tinkerbench.BenchmarkInsertion.getHost;
import static com.aerospike.tinkerbench.BenchmarkInsertion.getPort;

public class CLI {
    public static void main(String[] args) throws RunnerException, IOException {
            getHost();
            getPort();

            org.openjdk.jmh.Main.main(args);
    }
}

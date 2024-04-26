package com.aerospike.tinkerbench.cli;

import com.aerospike.tinkerbench.benchmarks.TinkerBench;
import com.aerospike.tinkerbench.benchmarks.identity_schema.BenchmarkShortRead;
import com.aerospike.tinkerbench.benchmarks.identity_schema.BenchmarkStitching;
import com.aerospike.tinkerbench.util.BenchmarkUtil;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class CLI {
    private static final Set<Class> benchmarks = Set.of(BenchmarkShortRead.class, BenchmarkStitching.class);

    public static void main(final String[] args) throws RunnerException, IOException {
        if (args.length == 0 || args.length == 1 &&
                (args[0].equals("-h") || args[0].equals("--help"))) {
            BenchmarkUtil.printUsage();
            System.exit(0);
        }
        System.out.println(
                        "                                                                                                                                         \n" +
                        "                                                                      ▓                                                                 \n" +
                        "                                                                    ▓▓▓                                                                  \n" +
                        "                                                          ▓▓      ▓▓▒▓▓                                                                  \n" +
                        "                                                          ▓▓▓   ▓▓▒░░▓▓                                                                  \n" +
                        "                                                   ▓▓▓    ▓░▒▓ ▓▓░░░░▓▓                                                                  \n" +
                        "                                                    ▓░▒▓▓▓▓░░▒▓▓░░░░░▓██▓▓▓ ▓▓▓▓                                                         \n" +
                        "                                 ▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓█  ▓▒░░░▓▓░░░░▓░░░░░▓▓▒░▓▓▓░░▓▓                                                         \n" +
                        "                                 ▓▓░░░▒▒▒▒░░░░░░░▒▒▓▓▓▓▒░▒▓░░░░░░░░░░░░░░▓▒░░░▓▓▓▓▓▓▓▓▓▒▒▒▒▓▓                                            \n" +
                        "                                  ▓▒░░░░░░▒▒▒▒░░░░░░░░░░▒▓▓░░░░░░░░░░░░░▒▓▓▓▓▒░░░░░░▒▒▒▒░░▓▓                                             \n" +
                        "                                   ▓▒░░░░░░░▒▒▒▒░░░░░░░░░░░░░░░░░░░░░░░░░▒░░░░░░▒▒▒▒▒░░░░▓▓                                              \n" +
                        "                                    ▓▓░░░░░░░░░▒▒▒░░░░░░░░▓▓▓█▓▒░░░░░░░░░░░░░░░▒▒▒░░░░░░▓▓                                               \n" +
                        "                                     ▓▓░░░░░░░░░▒▒░░░░░░▓░  ▒▒  ▒▒░░░░░░▓▓▓░░▒▒▒░░░░░░░▓▓                                                \n" +
                        "                                      ▓▓▓░░░░░░░░░░░░░░▓ ░░▓░ ▓  ░▓░░░░▒▓▒░▒▓▒░░░░░░░▒▓                                                  \n" +
                        "                                        ▓▓▓░░░░░░░░░░░▒▒  ░▓▓▓▓   ▓▒░░░▒▓▓▓▓▒▒░░░░░▒▓▓                                                   \n" +
                        "                                           ▓▓▓▓▓▓░░░░░░▒▓▒░ ░░    ░▒░░░▒▓▓▓░░▒░░░▒▓▓                                                     \n" +
                        "                                                ▓▒░░░░░░░░░░▒▒▒▓▓▓▒░░░░░░░▒▒░▓▓▓▓▓                                                       \n" +
                        "                                                █▓░░░░░▓░░░░░░░░░░░░░░░░░░░░░▓▓                                                          \n" +
                        "                                                  ▓▓░░░░▓▓▒▒░░░░░░░░░░░░░░▒▓▒▓█                                                          \n" +
                        "                                                   ▓▓▓░░░░▓░▒▓▒▒▓▓▓▓▓▓▓▒▒░▒▓▓                                                            \n" +
                        "                                            ▓▓▓▒▒▒▒▓▓▓▓▓▒░░▓░░░░░░░░░░░▒▓▓▓                                                              \n" +
                        "                                          ▓▓▒░░░░░░░░░░░▒░░░░░░░░░░▓▓▓▓▓ ▓▓▒▒▒▓▓▓                                                        \n" +
                        "                                         ▓▓░░░░░░▓▓▓▓▓█▓▓░░░░░░░░░░▒▒▓▓ ▓▓░░░░░░▒▓                                                       \n" +
                        "                                         ▓▒░░░░░░░░▓▓  ▓▓░░░░░░░░░░▒▒░▒▓▓▓░░░░░░░█▓                                                      \n" +
                        "                                         ▓▓░░░░░░░░▒▓ ▓▓░░░░░░░░░░░▒▒░░░░▒░░░░░░▒▓                                                       \n" +
                        "                                          ▓▓░░░░░░▒▓▓▓▓░░░░░░░░░░░▒▒▓▓▓▒░░░░░░░▒▓▓                                                       \n" +
                        "                                            ▓▓▓▓▓▓▓▓▒▒░░░░░░░░░░░▒▒▒▓▓ █▓▓▓▓▓▓▓▓                                                         \n" +
                        "                                         █▓▓▓▓▓  ██░░░░░░░░░░░░░▒▒▒▓▓█                                                                   \n" +
                        "                                     ▓▓▓▓▒░░░░▒▓ ▓▒░░░░░░░░░░░░▒▒▒▒░░▒▓▓█                                                                \n" +
                        "                                   ▓▓▒░░░░░░░░▓▓▓▒░░░░░░░░░▒▒▒▒▒▒░░░░░░░▓▓                                                               \n" +
                        "                                  █▓▒░░░░░░░░▒▓▒░░░░░░░▒▒▓▓▓▓▓▓▓▓▒▒░░░░░░▓▓▓▓▓▓▓▓▓                                                       \n" +
                        "                                  ▓▓░░░░░░░░░░░░░░░░░░▒▓           ▓▓▓░░░░░░░░░░░▒▓▓                                                     \n" +
                        "                                  ▓▓░░░░░░▒▓▓▒░░░░░░▒▓▓▓             ▓▓░░░░░░░░░░░▓▓                                                     \n" +
                        "                                   ▓▓▓▒▓▓▓█  █▓▓▓▓▓▓▓                ▓▓░░░░░░░░░▒▓▓                                                      \n" +
                        "                                                                     ▓▒░░░░░░░▒▓▓                                                        \n" +
                        "                                                                     ▓▓░░▒▒▓▓▓█                                                          \n" +
                        "                                                                       █▓█                                                               \n" +
                " _____          _             _                     _     \n" +
                "|_   _|(_)     | |           | |                   | |    \n" +
                "  | |   _ _ __ | | _____ _ __| |__   ___ _ __   ___| |__  \n" +
                "  | |  | | '_ \\| |/ / _ \\ '__| '_ \\ / _ \\ '_ \\ / __| '_ \\ \n" +
                "  | |  | | | | |   <  __/ |  | |_) |  __/ | | | (__| | | |\n" +
                "  \\_/  |_|_| |_|_|\\_\\___|_|  |_.__/ \\___|_| |_|\\___|_| |_|");
        System.out.println("Arguments provided: " + Arrays.toString(args));
        for (final Class benchmark : benchmarks) {
            if (benchmark.getSimpleName().equals(args[0])) {
                BenchmarkUtil.runBenchmark(benchmark);
                System.exit(0);
            }
        }
        System.out.println("Failed to find benchmark with name: " + args[0]);
        System.exit(1);
    }
}

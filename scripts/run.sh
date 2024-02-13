#!/usr/bin/env bash
THREADS=8

HOST=localhost PORT=8182 java \
  -jar target/tinkerBench-1.0-SNAPSHOT-jar-with-dependencies.jar \
  -t$THREADS

#!/usr/bin/env bash
THREADS=8

HOST=localhost PORT=8182 java \
  -jar bin/tinkerbench.jar \
  -t$THREADS

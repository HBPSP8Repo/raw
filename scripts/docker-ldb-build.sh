#!/bin/bash
# Figure out where RAW is installed
RAW_ROOT="$(cd "`dirname "$0"`"/..; pwd)"

cd ${RAW_ROOT}/executor/src/test/docker/ldb/
docker build -t raw/ldb .

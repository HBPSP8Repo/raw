#!/bin/bash
set -e
# Figure out where RAW is installed
RAW_ROOT="$(cd "`dirname "$0"`"/..; pwd)"
DOCKER_ROOT="${RAW_ROOT}/executor/src/test/docker/ldb-frontend/"
cd "$DOCKER_ROOT"
docker build -t nfsantos/ldb-frontend .

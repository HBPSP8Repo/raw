#!/bin/bash
# Figure out where RAW is installed
RAW_ROOT="$(cd "`dirname "$0"`"/..; pwd)"

touch.exe ${RAW_ROOT}/executor/src/test/docker/ldb/ldb-install.sh
${RAW_ROOT}/bin/docker-ldb-build.sh
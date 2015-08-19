#!/bin/bash
# Figure out where RAW is installed
RAW_ROOT="$(cd "`dirname "$0"`"/..; pwd)"

LDB_INSTALL=${RAW_ROOT}/executor/src/test/docker/ldb/ldb-install.sh
touch $LDB_INSTALL
${RAW_ROOT}/scripts/docker-ldb-build.sh

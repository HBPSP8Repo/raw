#!/bin/bash
set -e
# Figure out where RAW is installed
RAW_ROOT="$(cd "`dirname "$0"`"/..; pwd)"

cd "${RAW_ROOT}/executor/src/test/docker/ldb/"




LSB_SRC_RELATIVE_DIR=".src-ldb"
LDB_SRC_DIR=${RAW_ROOT}/${LSB_SRC_RELATIVE_DIR}
if [ -d "$LDB_SRC_DIR" ]; then
    pushd $LDB_SRC_DIR
    git pull
    popd
else
    git clone git://github.com/raw-db/ldb.git ${LSB_SRC_RELATIVE_DIR}
fi

docker build -t raw/ldb .

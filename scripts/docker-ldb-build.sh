#!/bin/bash
set -e
# Figure out where RAW is installed
RAW_ROOT="$(cd "`dirname "$0"`"/..; pwd)"
DOCKER_ROOT="${RAW_ROOT}/executor/src/test/docker/ldb/"
cd "$DOCKER_ROOT"

LSB_SRC_RELATIVE_DIR=".src-ldb"
LDB_SRC_DIR=${DOCKER_ROOT}/${LSB_SRC_RELATIVE_DIR}
echo "Repository location: $LDB_SRC_DIR"
if [ -d "$LDB_SRC_DIR" ]; then
    echo "Updating repository"
    pushd $LDB_SRC_DIR
    git pull
    popd
else
    echo "Cloning LDB repository"
    git clone git://github.com/raw-db/ldb.git ${LSB_SRC_RELATIVE_DIR}
fi

docker build -t raw/ldb .

#!/bin/bash
pushd executor/src/test/docker/ldb/
docker build -t raw/ldb .
popd

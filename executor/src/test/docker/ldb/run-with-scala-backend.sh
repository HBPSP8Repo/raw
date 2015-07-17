#!/bin/bash
# This first argument is the host of the RAW executor.
if [ $# -eq 0 ]; then
    HOST="localhost"
else
    HOST=$1
fi

ARGS="--scala-url=http://${HOST}:54321/execute -S --schema /raw/schema.odl"
echo "Starting OQL server with arguments $ARGS"
python server.py $ARGS

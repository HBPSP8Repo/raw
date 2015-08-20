#!/bin/bash
set -e

cd /raw/ldb/utils/pyserver

# This first argument, if present, is the dataset. If none is give, use the already preloaded dataset
# Options: pubs, pubs-small
if [ $# -ne 0 ]; then
    DATASET=$1
    if [ $DATASET != "publications" ]; then
        directory="data/${DATASET}/*"
        python load_files.py -b ${directory}
    fi
fi

echo "Starting OQL server with arguments $ARGS"
python server.py

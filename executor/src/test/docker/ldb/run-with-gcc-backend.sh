#!/bin/bash
set -e

cd /raw/ldb/utils/pyserver

# This first argument, if present, is the dataset. If none is give, use the already preloaded dataset
# Options: pubs, pubs-small
if [ $# -ne 0 ]; then
    DATASET=$1
    # The all dataset is preloaded and contains the publications and patients datasets
    if [[ $DATASET != "all" && $DATASET != "publications" && $DATASET != "patients" ]]; then
        directory="data/${DATASET}/*"
        python load_files.py -b ${directory}
    fi
fi

echo "Starting OQL server with arguments $ARGS"
python server.py

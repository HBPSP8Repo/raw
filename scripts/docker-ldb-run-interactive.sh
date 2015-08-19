#!/bin/bash
# The double slashes are needed to avoid mingw path conversions. 
# Seems it also works in Linux
WEB_SERVER_PORT=5001
docker run -it --rm -p ${WEB_SERVER_PORT}:5000 --workdir=//raw/ldb raw/ldb //bin/bash

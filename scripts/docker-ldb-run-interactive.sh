#!/bin/bash
# The double slashes are needed to avoid mingw path conversions. 
# Seems it also works in Linux
docker run -it --rm --entrypoint=//bin/bash raw/ldb

#!/bin/bash
set -e

# On Linux, the web server port is mapped to the localhost interface. On Windows it is mapped into the boot2docker VM IP
if [[ $(uname -s) == CYGWIN* || $(uname -s) == MINGW* ]]; then
	WEB_SERVER_IP=$(boot2docker ip)
else
	WEB_SERVER_IP=localhost
fi

WEB_SERVER_PORT=5001

echo "Starting web server on address: ${WEB_SERVER_IP}:${WEB_SERVER_PORT}/static/index.html"
docker run -it --rm -p ${WEB_SERVER_PORT}:5000 --entrypoint=//usr/bin/python raw/ldb server.py

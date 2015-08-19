#!/bin/bash
set -e

# The host that the ldb server should use to contact the scala executor. 
# On a Linux machine, this can be localhost. But when using boot2docker, the localhost that the container sees
# is the virtual interface created by VirtualBox. So we have to specify the external facing IP address of the host.
if [[ $(uname -s) == CYGWIN* || $(uname -s) == MINGW* ]]; then
	# WARN: This script takes the IP of the first interface listed by ifconfig. It may not be the external IP
	SCALA_SERVER_HOST=$(ipconfig | grep "IPv4 Address" | head -n 1 | cut -c 40-55)
	WEB_SERVER_IP=$(boot2docker ip)
else
	SCALA_SERVER_HOST=$(/sbin/ifconfig docker0|grep 'inet addr' | sed 's/inet addr:\([0-9\.]*\).*/\1/')
	WEB_SERVER_IP=localhost
fi

WEB_SERVER_PORT=5001

echo "Starting web server on address: ${WEB_SERVER_IP}:${WEB_SERVER_PORT}/static/index.html"
echo "Scala executor host: ${SCALA_SERVER_HOST}"
# Entrypoint has a funny syntax when running with arguments. A command like "xyz -a -b foobar",
# must be specified in this format: --entrypoint=xyz <dockerImage> -a -b foobar. 
# https://docs.docker.com/reference/run/#entrypoint-default-command-to-execute-at-runtime
docker run -it --rm -p ${WEB_SERVER_PORT}:5000 raw/ldb //raw/scripts/run-with-scala-backend.sh ${SCALA_SERVER_HOST}

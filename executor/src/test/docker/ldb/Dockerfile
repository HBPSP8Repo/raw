FROM ubuntu:14.04
MAINTAINER Nuno Santos "nfssantos@gmail.com"

# Install software 
RUN apt-get update && apt-get install -y \
    git \
    autoconf \
    libtool \
    g++ \
    gcc \
    make \
    libc-dev \
    pkg-config \
    bison \
    flex \
    python-minimal \
    python-flask \
    vim

# ssh config for github
RUN mkdir /root/.ssh/
ADD id_rsa /root/.ssh/id_rsa
RUN touch /root/.ssh/known_hosts && ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN mkdir /raw
# Install the dependencies
ADD bdwgc-install.sh /tmp/
RUN bash /tmp/bdwgc-install.sh

ADD shore-mt-install.sh /tmp/
RUN bash /tmp/shore-mt-install.sh

ADD ldb-install.sh /tmp/
RUN bash /tmp/ldb-install.sh
EXPOSE 5000	

# the server.py script must be executed from this directory, because it uses relative 
# paths to find other files.
WORKDIR /raw/ldb/utils/pyserver
ADD schema.odl /raw/
ENTRYPOINT ["/usr/bin/python", "server.py", "--schema=/raw/schema.odl"]

FROM ubuntu:14.04
MAINTAINER Nuno Santos "nfssantos@gmail.com"

# Install software 
RUN apt-get update && apt-get install -y \
    autoconf \
    bison \
    cmake \
    cscope \
    flex \
    g++ \
    gcc \
    git \
    indent \
    libc-dev \
    libgc1c2 \
    libgc-dev \
    libtool \
    make \
    pkg-config \
    python-cxx-dev \
    python-flask \
    python-minimal \
    python-pandas \
    python-pip \
    vim

# ssh config for github
RUN mkdir /root/.ssh/
ADD id_rsa /root/.ssh/id_rsa
RUN touch /root/.ssh/known_hosts && ssh-keyscan github.com >> /root/.ssh/known_hosts

RUN mkdir /raw

ADD shore-mt-install.sh /tmp/
RUN bash /tmp/shore-mt-install.sh

# LDB. Will copy the LDB source code from the host directory /tmp/ldb
RUN mkdir /raw/ldb
RUN mkdir /raw/ldbstore
RUN mkdir /raw/ldbstore/log
ADD Config ldbconfig /raw/ldb/
ADD .src-ldb/ /raw/ldb
RUN cd /raw/ldb && make

EXPOSE 5000	

# the server.py script must be executed from this directory, because it uses relative
# paths to find other files.
WORKDIR /raw/ldb/utils/pyserver

ADD schema.odl /raw/

RUN mkdir /raw/scripts
ADD run-with-*-backend.sh init-schema.sh /raw/scripts/

ADD data /raw/ldb/utils/pyserver/data/
RUN python load_files.py -b data/publications/*


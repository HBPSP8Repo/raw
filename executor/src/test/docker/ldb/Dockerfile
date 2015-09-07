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

RUN mkdir /raw

ADD shore-mt-install.sh /tmp/
RUN bash /tmp/shore-mt-install.sh

# LDB. Will copy the LDB source code from the host directory /tmp/ldb
RUN mkdir /raw/ldb && mkdir /raw/ldbstore && mkdir /raw/ldbstore/log
ADD Config ldbconfig /raw/ldb/
ADD .src-ldb/ /raw/ldb
RUN cd /raw/ldb && make

EXPOSE 5000

# the server.py script must be executed from this directory, because it uses relative
# paths to find other files.
WORKDIR /raw/ldb

RUN mkdir /raw/scripts
ADD run-with-*-backend.sh /raw/scripts/

RUN pip install flask-cors dropbox
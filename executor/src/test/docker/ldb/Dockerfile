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
    vim && apt-get clean

# Create all directories for shore and for LDB
RUN mkdir /opt/shoremt && mkdir -p /var/opt/shoremt/log && mkdir -p /opt/ldb

# Clone, compile and install shore.
ADD shore-mt-install.sh /tmp/
RUN bash /tmp/shore-mt-install.sh

# Copy the LDB source code from the host directory
ADD Config ldbconfig /opt/ldb/
ADD .src-ldb/ /opt/ldb
RUN cd /opt/ldb && make

WORKDIR /opt

RUN mkdir /opt/scripts
ADD run-with-*-backend.sh /opt/scripts/

RUN pip install flask-cors dropbox

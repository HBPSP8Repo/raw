FROM nfsantos/ldb
MAINTAINER Nuno Santos "nfssantos@gmail.com"

# the server.py script must be executed from this directory, because it uses relative
# paths to find other files.
WORKDIR /raw/ldb/utils/pyserver

ADD data /raw/ldb/utils/pyserver/data/
RUN python load_files.py -b data/all/*

ADD schema.odl /raw/
ADD init-schema.sh /raw/scripts/

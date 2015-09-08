FROM nfsantos/ldb
MAINTAINER Nuno Santos "nfssantos@gmail.com"

RUN ["/raw/ldb/bin/odl", "format"]

EXPOSE 5001
WORKDIR /raw/ldb/wn
CMD [ "python", "server.py", "-c" ]

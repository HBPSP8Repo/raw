FROM nfsantos/ldb
MAINTAINER Nuno Santos "nfssantos@gmail.com"

RUN ["/opt/ldb/bin/odl", "format"]

EXPOSE 5001
WORKDIR /opt/ldb/wn
CMD [ "python", "server.py", "-c" ]

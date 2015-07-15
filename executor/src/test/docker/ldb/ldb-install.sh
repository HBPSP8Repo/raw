#!/bin/bash
set -e

cd /raw
git clone git://github.com/raw-db/ldb.git
cd ldb
cat > Config << EOF
# LDB
LDBDIR = /raw/ldb

# Garbage Collector
GCDIR = /raw/bdwgc

# Shore-MT
SHORE = /raw/shoremt
EOF

mkdir /raw/ldbstore
mkdir /raw/ldbstore/log

cat > ldbconfig << EOF
# Options Settings for the LDB/SHORE storage manager 
         
# set the location of the diskrw program
#?.server.*.sm_diskrw: /home/miguel/shore-interim-3-pre/installed/bin/diskrw
#?.server.*.sm_diskrw: /home/miguel/shore-interim-3/installed/bin/diskrw
#?.server.*.sm_diskrw: /home/miguel/shore-storage-manager-5.0.3/installed/bin/diskrw
 
# the directory with the log files (must be created by administrator)
?.server.*.sm_logdir: /raw/ldbstore/log

# the database volume (this file is created and formated automatically by 'odl -build')
?.server.*.sm_volume: /raw/ldbstore/database

# set the buffer pool size for server
?.server.*.sm_bufpoolsize: 320000

# the size of the database volume in Kbytes
?.server.*.sm_volumesize: 1000000

# the maximum size that a log partition may reach
?.server.*.sm_logsize: 1000000
EOF

make

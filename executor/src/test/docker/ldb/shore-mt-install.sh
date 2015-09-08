#!/bin/bash
set -e

cd /tmp
git clone git://github.com/raw-db/shore-mt.git
cd shore-mt
./bootstrap
./configure --enable-dbgsymbols --prefix=/opt/shoremt
make -j4
make install
cd ..
rm -rf shore-mt

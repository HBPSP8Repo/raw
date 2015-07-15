#!/bin/bash
set -e

cd /raw
git clone git://github.com/raw-db/shore-mt.git
cd shore-mt
git apply <<EOF
diff --git a/configure.ac b/configure.ac
index a04bed1..227996f 100644
--- a/configure.ac
+++ b/configure.ac
@@ -26,7 +26,7 @@ AM_INIT_AUTOMAKE([-Wall -Werror foreign])
 ## This package uses an assembler.
 ## Define CCAS and CCASFLAGS 
 AM_PROG_AS
-#AM_PROG_AR
+AM_PROG_AR
 
 # Checks for programs.
 ## preprocessor required
EOF
./bootstrap
./configure --enable-dbgsymbols --prefix=/raw/shoremt
make -j4
make install

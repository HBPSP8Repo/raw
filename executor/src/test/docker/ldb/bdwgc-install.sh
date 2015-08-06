#!/bin/bash
set -e

cd raw
git clone git://github.com/ivmai/bdwgc.git
cd bdwgc/
git checkout 7aba59b5853330c9368bc16dd606e1617c704334
git clone git://github.com/ivmai/libatomic_ops.git
cd libatomic_ops
git checkout 437c10b73ae4b83306f77c851cf8385172ae3759
cd ..
autoreconf -vif
automake --add-missing
./configure --enable-threads=posix --enable-thread-local-alloc --enable-parallel-mark --enable-cplusplus --enable-static --disable-shared
git apply <<EOF
diff --git a/misc.c b/misc.c
index 93e8665..69e072d 100644
--- a/misc.c
+++ b/misc.c
@@ -1259,6 +1259,7 @@ GC_API void GC_CALL GC_init(void)
     GC_is_initialized = TRUE;
 #   if defined(GC_PTHREADS) || defined(GC_WIN32_THREADS)
         GC_thr_init();
+        usleep(5000);
 #   endif
     COND_DUMP;
     /* Get black list set up and/or incremental GC started */
EOF
make
# Do not run because if some checks fail, it will stop the build of the docker image
# make check

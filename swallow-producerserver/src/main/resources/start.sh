#!/bin/bash

PRGDIR=`dirname "$0"`
if [ ! -d "/data/applogs/swallow" ] ; then
  mkdir -p "/data/applogs/swallow"
fi

JAVA_OPTS="-cp ${PRGDIR}/.:${PRGDIR}/* -server -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9013 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -Xloggc:/data/applogs/swallow/swallow-producerserver-gc.log"
MAIN_CLASS="com.dianping.swallow.producerserver.bootstrap.ProducerServerBootstrap"

STD_OUT="/data/applogs/swallow/swallow-producerserver-std.out"
echo "starting..."
echo "output: $STD_OUT"
exec java $JAVA_OPTS $MAIN_CLASS > "$STD_OUT" 2>&1 &

#!/bin/bash

PRGDIR=`dirname "$0"`

JAVA_OPTS="-server -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9011 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -cp ${PRGDIR}/.:${PRGDIR}/*"
MAIN_CLASS="com.dianping.swallow.producerserver.bootstrap.ProducerServerBootstrap"

STD_OUT="/data/applogs/swallow/swallow-producerserver-std.out
echo "starting..."
echo "output: $STD_OUT"
exec java $JAVA_OPTS $MAIN_CLASS >> "$STD_OUT" 2>&1 &

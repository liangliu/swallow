@echo off

set JAVA_OPTS=-server -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9011 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -cp .:*
set MAIN_CLASS=com.dianping.swallow.producerserver.bootstrap.ProducerServerBootstrap

java %JAVA_OPTS% %MAIN_CLASS%
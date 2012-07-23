#!/bin/bash

usage(){
    echo "  Usage:"
    echo "     use '$0' or '$0 master'  to start as master."
    echo "     use '$0 slave' to start as slave."
    exit 1
}
script_path=`dirname "$0"`
MASTER_JMX_PORT=9011
SLAVE_JMX_PORT=9012
JAVA_OPTS="-server -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -cp ${script_path}/.:${script_path}/*"
MASTER_JAVA_OPTS="-Dcom.sun.management.jmxremote.port=${MASTER_JMX_PORT} ${JAVA_OPTS}"
SLAVE_JAVA_OPTS="-Dcom.sun.management.jmxremote.port=${SLAVE_JMX_PORT} ${JAVA_OPTS}"
MASTER_CLASS="com.dianping.swallow.consumerserver.bootstrap.MasterBootStrap"
SLAVE_CLASS="com.dianping.swallow.consumerserver.bootstrap.SlaveBootStrap"

if [ "$1" == "master" ] || [ "$1" == "" ]; then
    echo "starting as master ..."
    java $MASTER_JAVA_OPTS $MASTER_CLASS
elif [ "$1" == "slave" ]; then
    echo "starting as slave ..."
    java $SLAVE_JAVA_OPTS $SLAVE_CLASS
else
    echo "Your input is not corrent!"
    usage
fi
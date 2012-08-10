#!/bin/bash
PRGDIR=`dirname "$0"`
LOCAL_IP=`ifconfig eth0 | grep 'inet addr:' | cut -d: -f2 | awk '{ print $1}'`
if [ ! -d "/data/applogs/swallow" ] ; then
  mkdir -p "/data/applogs/swallow"
fi
usage(){
    echo "  Usage:"
    echo "     use '$0 master' to start as master."
    echo "     use '$0 slave [<masterIp>]' to start as slave."
    exit 1
}
MASTER_JMX_PORT=9011
SLAVE_JMX_PORT=9012
JAVA_OPTS="-cp ${PRGDIR}/.:${PRGDIR}/* -server -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -XX:+PrintGCDetails -XX:+PrintGCTimeStamps"
MASTER_JAVA_OPTS="${JAVA_OPTS} -Dmaster.or.slave=master -Dcom.sun.management.jmxremote.port=${MASTER_JMX_PORT} -Xloggc:/data/applogs/swallow/swallow-consumerserver-master-gc.log"
SLAVE_JAVA_OPTS="${JAVA_OPTS} -Dmaster.or.slave=slave -Dcom.sun.management.jmxremote.port=${SLAVE_JMX_PORT} -Xloggc:/data/applogs/swallow/swallow-consumerserver-slave-gc.log"
MASTER_CLASS="com.dianping.swallow.consumerserver.bootstrap.MasterBootStrap"
SLAVE_CLASS="com.dianping.swallow.consumerserver.bootstrap.SlaveBootStrap"

if [ "$1" == "master" ]; then
    STD_OUT="/data/applogs/swallow/swallow-consumerserver-master-std.out"
    MASTER_JAVA_OPTS="${MASTER_JAVA_OPTS} -DmasterIp=$LOCAL_IP"
    echo "starting as master(masterIp is $LOCAL_IP ) ..."
    echo "output: $STD_OUT"
    exec java $MASTER_JAVA_OPTS $MASTER_CLASS > "$STD_OUT" 2>&1 &
elif [ "$1" == "slave" ]; then
    if [ "$2" != "" ]; then
       echo "masterIp option: $2"
       echo $2 |grep "^[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}\.[0-9]\{1,3\}$" > /dev/null
       if [ $? == 1 ]; then
          echo "'$2' is an illegal ip address!"
          usage
          exit 1
       fi
       SLAVE_JAVA_OPTS="${SLAVE_JAVA_OPTS} $2"
    else
       echo "no masterIp option (e.g. '<masterIp>'), would use the 'masterIp' property in swallow-consumerserver.properties "
    fi
    STD_OUT="/data/applogs/swallow/swallow-consumerserver-slave-std.out"
    echo "starting as slave ..."
    echo "output: $STD_OUT"
    exec java $SLAVE_JAVA_OPTS $SLAVE_CLASS > "$STD_OUT" 2>&1 &
else
    echo "Your input is not corrent!"
    usage
fi
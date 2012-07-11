#!/bin/bash

usage(){
    echo "  Usage:"
    echo "     use '$0' or '$0 master'  to start as master."
    echo "     use '$0 slave' to start as slave."
    exit 1
}

JAVA_OPTS="-Xmx2g -cp .:*"
MASTER_CLASS="com.dianping.swallow.consumerserver.bootstrap.MasterBootStrap"
SLAVE_CLASS="com.dianping.swallow.consumerserver.bootstrap.SlaveBootStrap"

if [ "$1" == "master" ] || [ "$1" == "" ]; then
    echo "starting as master ..."
    java $JAVA_OPTS $MASTER_CLASS
elif [ "$1" == "slave" ]; then
    echo "starting as slave ..."
    java $JAVA_OPTS $SLAVE_CLASS
else
    echo "Your input is not corrent!"
    usage
fi
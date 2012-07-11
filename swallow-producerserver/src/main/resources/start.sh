#!/bin/bash

JAVA_OPTS="-Xmx2g -cp .:*"
MAIN_CLASS="com.dianping.swallow.producerserver.bootstrap.ProducerServerBootstrap"

java $JAVA_OPTS $MAIN_CLASS
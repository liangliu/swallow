@echo off

set JAVA_OPTS=-Xmx2g -cp .;*
set MAIN_CLASS=com.dianping.swallow.producerserver.bootstrap.ProducerServerBootstrap

java %JAVA_OPTS% %MAIN_CLASS%
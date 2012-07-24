@echo off

set MASTER_JMX_PORT=9011
set SLAVE_JMX_PORT=9012
set JAVA_OPTS=-server -Xms512m -Xmx2g -XX:+HeapDumpOnOutOfMemoryError -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false -cp .:*
set MASTER_JAVA_OPTS=-Dcom.sun.management.jmxremote.port=%MASTER_JMX_PORT% %JAVA_OPTS%
set SLAVE_JAVA_OPTS=-Dcom.sun.management.jmxremote.port=%SLAVE_JMX_PORT% %JAVA_OPTS%
set MASTER_CLASS=com.dianping.swallow.consumerserver.bootstrap.MasterBootStrap
set SLAVE_CLASS=com.dianping.swallow.consumerserver.bootstrap.SlaveBootStrap

if "%1" EQU "master"   ( 
   echo "starting as master ..."
   java %MASTER_JAVA_OPTS% %MASTER_CLASS%
) else ( 
   if "%1" EQU "" (
      echo "starting as master ..."
      java %MASTER_JAVA_OPTS% %MASTER_CLASS%
   ) else (
      if "%1" EQU "slave" (
        echo "starting as slave ..."
        java %SLAVE_JAVA_OPTS% %SLAVE_CLASS%
      ) else (
        echo   Your input is not corrent!
        echo   Usage:
        echo      use '%0' or '%0 master'  to start as master.
        echo      use '%0 slave' to start as slave.
      )
   )
)


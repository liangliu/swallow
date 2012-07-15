@echo off

set JAVA_OPTS=-Xmx2g -cp .;*
set MASTER_CLASS=com.dianping.swallow.consumerserver.bootstrap.MasterBootStrap
set SLAVE_CLASS=com.dianping.swallow.consumerserver.bootstrap.SlaveBootStrap

if "%1" EQU "master"   ( 
   echo "starting as master ..."
   java %JAVA_OPTS% %MASTER_CLASS%
) else ( 
   if "%1" EQU "" (
      echo "starting as master ..."
      java %JAVA_OPTS% %MASTER_CLASS%
   ) else (
      if "%1" EQU "slave" (
        echo "starting as slave ..."
        java %JAVA_OPTS% %SLAVE_CLASS%
      ) else (
        echo   Your input is not corrent!
        echo   Usage:
        echo      use '%0' or '%0 master'  to start as master.
        echo      use '%0 slave' to start as slave.
      )
   )
)


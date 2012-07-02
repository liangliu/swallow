package com.dianping.swallow.consumerserver;

import org.jboss.netty.bootstrap.ServerBootstrap;

public interface Heartbeater {

   /**
    * master 心跳一次
    * 
    * @param ip
    */
   void beat(String ip);

   /**
    * 每隔checkInterval检查id的心跳状态，
    * 直到id停止心跳超过maxStopTime而且该方法从调用到现在的时间也超过了maxStopTime
    * 
    * @param ip
    * @param checkInterval
    * @param maxStopTime
    * @throws InterruptedException
    */
   void waitUntilStopBeating(String ip, long checkInterval, long maxStopTime) throws InterruptedException;

   void waitUntilBeginBeating(String ip, ServerBootstrap bootStrap, long checkInterval, long maxStopTime)
         throws InterruptedException;

}

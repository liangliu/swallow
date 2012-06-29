package com.dianping.swallow.producer;

/**
 * Producer的工作模式，同步/异步
 * 
 * @author tong.song
 */
public enum ProducerMode {
   /**
    * Producer工作模式：同步模式，APP等待远程调用结束
    */
   SYNC_MODE,
   /**
    * Producer工作模式：异步模式，APP等待SwallowMessage放入队列
    */
   ASYNC_MODE,
}

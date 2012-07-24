package com.dianping.swallow.consumerserver.buffer;

import java.util.concurrent.BlockingQueue;

public interface CloseableBlockingQueue<E> extends BlockingQueue<E> {

   /**
    * 关闭BlockingQueue占用的资源
    */
   void close();

   /**
    * 是否已经关闭BlockingQueue占用的资源
    */
   void isClosed();

}

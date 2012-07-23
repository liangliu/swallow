package com.dianping.swallow.common.internal.threadfactory;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.internal.monitor.ThreadStatusBean;

/**
 * 管理所有swallow线程，提供jmx监控
 * 
 * @author qing.gu
 */
public class MQThreadFactory implements ThreadFactory, Closeable {

   private static Logger                            LOG            = LoggerFactory.getLogger(MQThreadFactory.class);

   private static ThreadGroup                       topThreadGroup = new ThreadGroup("swallow-top");
   private final static String                      PREFIX_DEFAULT = "swallow-";
   private String                                   prefix;

   private List<WeakReference<Thread>>              threadList     = Collections
                                                                         .synchronizedList(new ArrayList<WeakReference<Thread>>());
   private ConcurrentHashMap<String, AtomicInteger> prefixToSeq    = new ConcurrentHashMap<String, AtomicInteger>();

   public MQThreadFactory(String namePrefix) {
      if(namePrefix != null) {
         this.prefix = namePrefix;
      } else {
         this.prefix = PREFIX_DEFAULT;
      }
      HawkJMXUtil.registerMBean("SwallowThreadStatusBean", new ThreadStatusBean());
   }

   public MQThreadFactory() {
      this(PREFIX_DEFAULT);
   }

   public static ThreadGroup getTopThreadGroup() {
      return topThreadGroup;
   }

   @Override
   public Thread newThread(Runnable r) {
      return newThread(r, "");
   }

   public Thread newThread(Runnable r, String threadNamePrefix) {
      prefixToSeq.putIfAbsent(threadNamePrefix, new AtomicInteger(1));
      Thread t = new Thread(topThreadGroup, r, prefix + threadNamePrefix
            + prefixToSeq.get(threadNamePrefix).getAndIncrement());
      threadList.add(new WeakReference<Thread>(t));
      return t;
   }

   @Override
   public void close() {
      for (WeakReference<Thread> ref : threadList) {
         Thread t = ref.get();
         if (t != null && t.isAlive()) {
            if (t instanceof Closeable) {
               try {
                  ((Closeable) t).close();
               } catch (Exception e) {
                  LOG.error("unexpected exception", e);
               }
            }
            t.interrupt();
         }
      }
   }

}

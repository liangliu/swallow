package com.dianping.swallow.common.internal.monitor;

import java.lang.Thread.State;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.dianping.hawk.jmx.HawkJMXUtil;
import com.dianping.swallow.common.internal.threadfactory.MQThreadFactory;

public class ThreadStatusBean {

   public Object getAllThreadInfo() {
      ThreadGroup topThreadGroup = MQThreadFactory.getTopThreadGroup();
      Thread[] threads = new Thread[topThreadGroup.activeCount()];
      topThreadGroup.enumerate(threads, false);
      List<List<Object>> result = new ArrayList<List<Object>>();
      for (int i = 0; i < threads.length; i++) {
         Thread t = threads[i];
         List<Object> tinfo = new ArrayList<Object>();
         tinfo.add(t.getName());
         tinfo.add(t.getState());
         tinfo.add(t.getStackTrace());
         result.add(tinfo);
      }
      return result;
   }

   public Map<String, Integer> getThreadState() {
      ThreadGroup tg = MQThreadFactory.getTopThreadGroup();
      Thread[] threads = new Thread[tg.activeCount()];
      tg.enumerate(threads, false);
      int newCount = 0;
      int runnableCount = 0;
      int blockedCount = 0;
      int waitingCount = 0;
      int timed_waitingCount = 0;
      int terminatedCount = 0;
      for (Thread t : threads) {
         State s = t.getState();
         switch (s) {
            case NEW:
               newCount++;
               break;
            case RUNNABLE:
               runnableCount++;
               break;
            case BLOCKED:
               blockedCount++;
               break;
            case WAITING:
               waitingCount++;
               break;
            case TIMED_WAITING:
               timed_waitingCount++;
               break;
            case TERMINATED:
               terminatedCount++;
               break;
         }
      }

      Map<String, Integer> stat2Cnt = new HashMap<String, Integer>();
      stat2Cnt.put("new", newCount);
      stat2Cnt.put("runnable", runnableCount);
      stat2Cnt.put("blocked", blockedCount);
      stat2Cnt.put("waiting", waitingCount);
      stat2Cnt.put("timed_waiting", timed_waitingCount);
      stat2Cnt.put("terminated", terminatedCount);

      return stat2Cnt;
   }

   public static void main(String[] args) throws InterruptedException {
      HawkJMXUtil.registerMBean(new ThreadStatusBean());
      Thread.sleep(1000000);
   }

}

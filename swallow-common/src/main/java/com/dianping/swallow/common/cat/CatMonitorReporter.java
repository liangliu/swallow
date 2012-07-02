package com.dianping.swallow.common.cat;

import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;

public class CatMonitorReporter extends TimerTask {

   private List<CatMonitorBean> catMonitorBeans;
   private long                 period = 5 * 60 * 1000; //默认是5分钟

   private Timer                t;

   public void setCatMonitorBeans(List<CatMonitorBean> catMonitorBeans) {
      this.catMonitorBeans = catMonitorBeans;
   }

   public void setPeriod(long period) {
      this.period = period;
   }

   public void start() {
      t = new Timer("Timer-CatMonitorReporter", true);
      t.schedule(this, 0, period);
   }

   public void cancle() {
      if (t != null) {
         t.cancel();
      }
   }

   @Override
   public void run() {
      if (this.catMonitorBeans == null)
         return;
      for (CatMonitorBean catMonitorBean : this.catMonitorBeans) {
         String type = catMonitorBean.getClass().getSimpleName();
         String name = type;
         Transaction t = Cat.getProducer().newTransaction(type, name);
         try {
            Event event = Cat.getProducer().newEvent(type, name);
            Map<String, Object> statusMap = catMonitorBean.getStatusMap();
            for (Map.Entry<String, Object> entry : statusMap.entrySet()) {
               event.addData(entry.getKey(), entry.getValue());
            }
            event.setStatus(Event.SUCCESS);
            event.complete();
            t.setStatus(Transaction.SUCCESS);
         } catch (Exception e) {
            t.setStatus(e);
         } finally {
            t.complete();
         }
      }

   }

}

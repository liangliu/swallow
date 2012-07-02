package com.dianping.swallow.common.cat;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.internal.DefaultEvent;

public class CatAdapter {

   public static void logEvent(Event event0) {
      Transaction t = Cat.getProducer().newTransaction(event0.getType(), event0.getName());
      try {
         Event event = Cat.getProducer().newEvent(event0.getType(), event0.getName());
         event.addData(((CharSequence)event0.getData()).toString());
         event.setStatus(Event.SUCCESS);
         event.complete();
         t.setStatus(Transaction.SUCCESS);
      } catch (Exception e) {
         t.setStatus(e);
      } finally {
         t.complete();
      }
   }

   public static void logEvent(String type, String name, String data) {
      Transaction t = Cat.getProducer().newTransaction(type, name);
      try {
         Event event = Cat.getProducer().newEvent(type, name);
         event.addData(data);
         event.setStatus(Event.SUCCESS);
         event.complete();
         t.setStatus(Transaction.SUCCESS);
      } catch (Exception e) {
         t.setStatus(e);
      } finally {
         t.complete();
      }
   }

   public static void main(String[] args) {
      Event event = new DefaultEvent("dd232", "dd");
      event.addData("id", 12345);
      event.addData("user", "john");
      logEvent(event);
   }

}

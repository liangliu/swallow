package com.dianping.swallow.common.cat;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;

public class CatLog4jAppender extends AppenderSkeleton {
   @Override
   protected void append(LoggingEvent event) {
      Transaction t = Cat.getProducer().newTransaction("errorlog", event.getLevel().toString());
      try {
         ThrowableInformation throwableInformation = event.getThrowableInformation();
         if (throwableInformation != null) {
            Cat.getProducer().logError(throwableInformation.getThrowable());
         } else {
            String msg = "[thread:" + event.getThreadName() + "][" + event.getLocationInformation().getClassName()
                  + " line:" + event.getLocationInformation().getLineNumber() + "] " + event.getRenderedMessage();
            Cat.getProducer().logEvent("errorlog", "errorlog", Event.SUCCESS, msg);
         }
         t.setStatus(Transaction.SUCCESS);
      } catch (Exception e) {
         t.setStatus(e);
      } finally {
         t.complete();
      }
   }

   @Override
   public void close() {
   }

   @Override
   public boolean requiresLayout() {
      return false;
   }
}

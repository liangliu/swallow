package com.dianping.swallow.common.internal.cat;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

import com.dianping.cat.Cat;
import com.dianping.cat.message.Message;
import com.dianping.cat.message.Transaction;

public class CatLog4jAppender extends AppenderSkeleton {
   @Override
   protected void append(LoggingEvent event) {
      String level = event.getLevel().toString();
      Transaction t = Cat.getProducer().newTransaction("log", level);
      try {
         ThrowableInformation throwableInformation = event.getThrowableInformation();
         if (throwableInformation != null) {
            Cat.getProducer().logError(throwableInformation.getThrowable());
         } else {
            String msg = "[thread:" + event.getThreadName() + "][" + event.getLocationInformation().getClassName()
                  + " line:" + event.getLocationInformation().getLineNumber() + "] " + event.getRenderedMessage();
            Cat.getProducer().logEvent("log", level, Message.SUCCESS, msg);
         }
         t.setStatus(Message.SUCCESS);
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

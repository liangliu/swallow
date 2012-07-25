package com.dianping.swallow.producer.cases;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.producer.exceptions.SendFailedException;
import com.dianping.swallow.producer.Producer;

public class MultipleThreadSend {
   Logger logger = LoggerFactory.getLogger(MultipleThreadSend.class);

   class tskSync implements Runnable {
      Producer producer;
      int      count;
      int      freq;
      String   pre;

      public tskSync(Producer producer, int count, int freq, String pre) {
         this.producer = producer;
         this.count = count;
         this.freq = freq;
         this.pre = pre;
      }

      @Override
      public void run() {
         int i = 0;
         if (count > 0) {
            for (i = 0; i < count; i++) {
               try {
                  producer.sendMessage(pre + ": " + ++i);
                  Thread.sleep(freq < 0 ? 0 : freq);
               } catch (SendFailedException e) {
                  logger.warn("Message sent failed, message ID=" + i);
               } catch (InterruptedException e) {
                  logger.warn("Sleep interrupted.");
                  System.exit(0);
               }
            }
         } else {
            while (true) {
               try {
                  producer.sendMessage(pre + ": " + ++i);
                  Thread.sleep(freq < 0 ? 0 : freq);
               } catch (SendFailedException e) {
                  logger.warn("Message sent failed, message ID=" + i);
               } catch (InterruptedException e) {
                  logger.warn("Sleep interrupted.");
                  System.exit(0);
               }
            }
         }
      }
   }
}

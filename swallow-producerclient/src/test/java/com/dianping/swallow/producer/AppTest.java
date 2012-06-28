package com.dianping.swallow.producer;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.dianping.swallow.producer.impl.ProducerImpl;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

/**
 * Unit test for simple App.
 */
public class AppTest extends TestCase {
   ProducerFactoryImpl pf = null;

   /**
    * Create the test case
    * 
    * @param testName name of the test case
    */
   public AppTest(String testName) {
      super(testName);
      try {
         pf = ProducerFactoryImpl.getInstance(5000);
      } catch (Exception e3) {
         System.out.println(e3.toString());
      }
   }

   /**
    * @return the suite of tests being tested
    */
   public static Test suite() {
      return new TestSuite(AppTest.class);
   }

   /**
    * Rigourous Test :-)
    */
   public void testApp() {
      assertTrue(true);
   }

   private class task implements Runnable {
      String content;

      public task(String content) {
         this.content = content;
      }

      @Override
      public void run() {
         Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();
         pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.ASYNC_MODE);
         pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);
         pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, true);
         
         ProducerImpl ps = null;
         
         try {
            ps = pf.getProducer(content, pOptions);
         } catch (Exception e2) {
            System.out.println(e2.toString());
         }
         try {
            while (true) {
               //			content += i++;
               System.out.println(ps.sendMessage(content));
               try {
                  Thread.sleep(2000);
               } catch (Exception e) {
                  // TODO: handle exception
               }
            }
         } catch (Exception e1) {
            System.out.println(e1.toString());
         }
      }
   }

   public void doTest() {
      for (int i = 0; i < 3; i++) {
         String newContent = "NO_" + i;
         Thread td = new Thread(new task(newContent));
         td.start();
      }
   }

   public static void main(String[] args) {
      new AppTest("111").doTest();
//      ProducerConfigure pc = new ProducerConfigure("producer.properties");
//      System.out.println(pc.getRemoteServiceTimeout());
//      System.out.println(pc.getThreadPoolSize());
//      System.out.println(pc.getDestinationName());
//      System.out.println(pc.getProducerModeStr());
//      System.out.println(pc.isContinueSend());
   }
}

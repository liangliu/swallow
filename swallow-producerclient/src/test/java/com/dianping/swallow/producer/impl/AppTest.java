package com.dianping.swallow.producer.impl;

import java.util.HashMap;
import java.util.Map;

import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.producer.exceptions.NullContentException;
import com.dianping.swallow.common.producer.exceptions.RemoteServiceDownException;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;
import com.dianping.swallow.producer.ProducerMode;
import com.dianping.swallow.producer.ProducerOptionKey;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;
import com.dianping.swallow.producer.impl.internal.ProducerImpl;

public class AppTest {
   private ProducerFactoryImpl producerFactory = null;
   private String              message         = "";

   //初始化
   public AppTest() {
      //初始化ProducerFactory
      try {
         producerFactory = ProducerFactoryImpl.getInstance();
      } catch (Exception e) {
      }
      //设置待发送的消息内容
      for (int i = 0; i < 10; i++) {
         message += "AAbbCCddEEffGGhhII jKKllMMnnOOppQQrr SttUUvvWWxxYYzz11@@33$$55^^77**99))aaeeeffggesswweedd!@#$%^&*()";
      }

   }

   private class TestTask implements Runnable {

      @Override
      public void run() {
         //设置Producer选项
         Map<ProducerOptionKey, Object> pOptions = new HashMap<ProducerOptionKey, Object>();

         pOptions.put(ProducerOptionKey.PRODUCER_MODE, ProducerMode.SYNC_MODE);
         pOptions.put(ProducerOptionKey.RETRY_TIMES, 2);
         pOptions.put(ProducerOptionKey.IS_ZIP_MESSAGE, true);

         pOptions.put(ProducerOptionKey.ASYNC_THREAD_POOL_SIZE, 5);
         pOptions.put(ProducerOptionKey.ASYNC_IS_CONTINUE_SEND, false);

         //设置发送消息时的选项
         Map<String, String> properties = new HashMap<String, String>();
         properties.put("zip", "false");

         //构造Producer
         ProducerImpl producer = null;
         try {
            producer = (ProducerImpl) producerFactory.getProducer(Destination.topic("xx"), pOptions);
         } catch (Exception e) {
            e.printStackTrace();
         }

         //发送消息设置
         final int MAX_NUM = 100; //可发送消息的最大数量
         int sentNum = 0; //已发送消息数量
         String strRet = ""; //发送消息的返回值

         //发送消息
//                  while (true) {
         for (int i = 0; i < MAX_NUM; i++) {
            try {
               //发送消息
               strRet = producer.sendMessage("" + (++sentNum));
            } catch (ServerDaoException e1) {
               e1.printStackTrace();
            } catch (FileQueueClosedException e1) {
               e1.printStackTrace();
            } catch (RemoteServiceDownException e1) {
               System.out.println("Network is down.");
            } catch (NullContentException e1) {
               e1.printStackTrace();
            }

            //发送频率
            try {
               Thread.sleep(1000);
            } catch (Exception e) {
            }

            //打印内容
            System.out.println(sentNum + ": " + strRet);
         }
      }
   }

   public void doTest() {
      final int THREAD_NUM = 1;//线程数量

      for (int i = 0; i < THREAD_NUM; i++) {
         Thread task = new Thread(new TestTask());
         task.start();
      }
   }

   public static void main(String[] args) {
      new AppTest().doTest();
   }

   public String getMessage() {
      return message;
   }

   public void setMessage(String message) {
      this.message = message;
   }
}

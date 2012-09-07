package com.dianping.swallow.example.producer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.producer.Producer;
import com.dianping.swallow.producer.ProducerConfig;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

/**
 * @rundemo_name 同步发送者例子(可输入)
 */
public class SyncProducerInputExample {

   public static void main(String[] args) throws Exception {
      ProducerConfig config = new ProducerConfig();
      Producer p = ProducerFactoryImpl.getInstance().createProducer(Destination.topic("example"), config);
      String CurLine = ""; // Line read from standard in

      System.out.println("输入要发送的消息 (type 'quit' to exit): ");
      InputStreamReader converter = new InputStreamReader(System.in);

      BufferedReader in = new BufferedReader(converter);

      while (!(CurLine.equals("quit"))) {
         CurLine = in.readLine();

         if (!(CurLine.equals("quit"))) {
            System.out.println("您发送的是: " + CurLine);
            p.sendMessage(CurLine);
         } else {
            System.exit(0);
         }
      }
   }

}

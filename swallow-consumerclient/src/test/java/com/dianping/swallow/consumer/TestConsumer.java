package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;

public class TestConsumer {

   //应该都是取自于lion
   public static String       swallowCAddress = "127.0.0.1:8081,127.0.0.1:8082";
   public static String       cid             = "zhangyu";
   public static Destination  dest            = Destination.topic("xx");
   public static ConsumerType consumerType    = ConsumerType.AT_LEAST;

   public static void main(String[] args) throws Exception {
      //TODO 通过spring使用的example
      //      final ConsumerClient cClient = new ConsumerClient(cid, dest, swallowCAddress);
      //      cClient.setConsumerType(consumerType);
      //      cClient.setListener(new MessageListener() {
      //
      //         @Override
      //         public void onMessage(MessageEvent e) {
      //            //用户得到SwallowMessage
      //            SwallowMessage swallowMessage = (SwallowMessage) ((PktMessage) e.getMessage()).getContent();
      //            Long messageId = swallowMessage.getMessageId();
      //            PktConsumerMessage consumermessage = new PktConsumerMessage(ConsumerMessageType.ACK, messageId, cClient.getNeedClose());
      //
      //            System.out.println(swallowMessage.getMessageId() + ":" + swallowMessage.getContent());
      //            e.getChannel().write(consumermessage);
      //            try {
      //               Thread.sleep(500);
      //            } catch (InterruptedException e1) {
      //               // TODO 使用LOG
      //               e1.printStackTrace();
      //            }
      //         }
      //
      //      });
      //      cClient.beginConnect();

   }
}

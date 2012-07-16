package com.dianping.swallow.consumer;

import com.dianping.swallow.common.message.Destination;

public interface ConsumerFactory {

   /**
    * 创建带特殊要求（ConsumerConfig）的consumer
    * @param dest
    * @param consumerId
    * @param config
    * @return
    */
   Consumer createConsumer(Destination dest, String consumerId, ConsumerConfig config);
   
   /**
    * 创建普通的consumer
    * @param dest
    * @param consumerId
    * @return
    */
   Consumer createConsumer(Destination dest, String consumerId);
   
}

package com.dianping.swallow.consumer;

import com.dianping.swallow.common.message.Destination;

public interface ConsumerFactory {

   Consumer createConsumer(Destination dest, String consumerId, ConsumerConfig config);
   
   Consumer createConsumer(Destination dest, String consumerId);
   
}

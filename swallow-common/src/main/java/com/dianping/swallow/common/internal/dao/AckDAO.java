package com.dianping.swallow.common.internal.dao;

public interface AckDAO {

   /**
    * 获取topicName和consumerId对应的最大的messageId
    */
   Long getMaxMessageId(String topicName, String consumerId);

   /**
    * 添加一条topicName，consumerId，messageId记录
    */
   void add(String topicName, String consumerId, Long messageId);

}

package com.dianping.swallow.common.dao;

public interface AckDAO<ID> {

   /**
    * 获取topicName和consumerId对应的最大的messageId
    */
   ID getMaxMessageId(String topicName, String consumerId);

   /**
    * 添加一条topicName，consumerId，messageId记录
    */
   void add(String topicName, String consumerId, ID messageId);

}

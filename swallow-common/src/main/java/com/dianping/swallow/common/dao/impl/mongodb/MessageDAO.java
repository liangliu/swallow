package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.List;

import com.dianping.swallow.common.message.SwallowMessage;

public interface MessageDAO<ID> {

   /**
    * 获取topic数据库的topicName集合/表里，size条messageId字段比messageId参数大的记录
    */
   List<SwallowMessage> getMessagesGreaterThan(String topicName, ID messageId, int size);

   /**
    * 获取topic数据库的topicName集合/表里，messageId字段最小的size条记录
    */
   List<SwallowMessage> getMinMessages(String topicName, int size);

   /**
    * 获取topic数据库的topicName集合/表里，最大的messageId字段
    * 
    * @param topicName
    * @return
    */
   Long getMaxMessageId(String topicName);

   /**
    * 往topic数据库的topicName集合/表里，插入一条信息
    */
   void saveMessage(String topicName, SwallowMessage message);

}

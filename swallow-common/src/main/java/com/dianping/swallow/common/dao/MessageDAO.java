package com.dianping.swallow.common.dao;

import java.util.List;

import com.dianping.swallow.common.message.SwallowMessage;

public interface MessageDAO {
   /**
    * 往topic数据库的topicName集合/表里，插入一条信息
    * 
    * @return
    */
   void saveMessage(String topicName, SwallowMessage message);

   /**
    * 获取topic数据库的topicName集合/表里，对应messageId字段的记录
    */
   SwallowMessage getMessage(String topicName, Long messageId);

   /**
    * 获取topic数据库的topicName集合/表里，size条messageId字段比messageId参数大的记录
    */
   List<SwallowMessage> getMessagesGreaterThan(String topicName, Long messageId, int size);

   /**
    * 获取topic数据库的topicName集合/表里，最大的messageId字段
    * 
    * @param topicName
    * @return
    */
   Long getMaxMessageId(String topicName);

   /**
    * 获取topic数据库的topicName集合/表里，messageId字段最大的消息
    * 
    * @param topicName
    * @return
    */
   SwallowMessage getMaxMessage(String topicName);

}

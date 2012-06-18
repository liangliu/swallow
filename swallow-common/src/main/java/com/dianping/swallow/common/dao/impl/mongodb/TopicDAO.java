package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.List;

import com.dianping.swallow.common.message2.SwallowMessage;

public interface TopicDAO<ID> {

   /**
    * 获取topic数据库的topicName集合/表里，size条messageId字段比messageId参数大的记录
    */
   List<SwallowMessage> getMessagesGreaterThan( ID messageId,String topicName,int size);
}

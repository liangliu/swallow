package com.dianping.swallow.consumerserver.buffer;

import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.consumer.MessageFilter;
import com.dianping.swallow.common.consumer.MessageFilter.FilterType;
import com.dianping.swallow.common.internal.dao.MessageDAO;
import com.dianping.swallow.common.internal.message.SwallowMessage;

public class MongoDBMessageRetriever implements MessageRetriever {
   private static final Logger LOG       = LoggerFactory.getLogger(MongoDBMessageRetriever.class);

   private int                 fetchSize = 100;                                                   //默认100条

   private MessageDAO          messageDAO;

   public void setMessageDAO(MessageDAO messageDAO) {
      this.messageDAO = messageDAO;
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   @Override
   public List retriveMessage(String topicName, Long messageId, MessageFilter messageFilter) {
      List maxIdAndMessages = messageDAO.getMessagesGreaterThan(topicName, messageId, fetchSize);

      Long maxMessageId = null;
      if (maxIdAndMessages != null && maxIdAndMessages.size() > 0) {
         //记录本次返回的最大那条消息的messageId
         maxMessageId = ((SwallowMessage) maxIdAndMessages.get(maxIdAndMessages.size() - 1)).getMessageId();
         //过滤type
         if (messageFilter != null && messageFilter.getType() == FilterType.InSet && messageFilter.getParam() != null
               && !messageFilter.getParam().isEmpty()) {
            Iterator<SwallowMessage> iterator = maxIdAndMessages.iterator();
            while (iterator.hasNext()) {
               SwallowMessage msg = iterator.next();
               if (!messageFilter.getParam().contains(msg.getType())) {
                  iterator.remove();
               }
            }
         }
         //无论最终过滤后messages.size是否大于0，都添加maxMessageId到返回集合
         maxIdAndMessages.add(0, maxMessageId);
      }

      if (LOG.isDebugEnabled()) {
         LOG.debug("fetched messages from mongodb, size:" + maxIdAndMessages.size());
         LOG.debug("messages:" + maxIdAndMessages);
      }

      //如果返回值messages.size大于0，则第一个元素一定是更新后的maxMessageId
      return maxIdAndMessages;
   }

   @Override
   public void setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
   }

}

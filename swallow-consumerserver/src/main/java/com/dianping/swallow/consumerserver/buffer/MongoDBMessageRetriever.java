package com.dianping.swallow.consumerserver.buffer;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
   public List retriveMessage(String topicName, Long messageId, MessageFilter messageFilter) throws Exception {
      List<SwallowMessage> messages;
      messages = messageDAO.getMessagesGreaterThan(topicName, messageId, fetchSize);
      //过滤type
      //TODO refactor filter logic to enable different filter type
      if (messageFilter != null && messageFilter.getType() == FilterType.InSet && !messageFilter.getParam().isEmpty() && messages!=null) {
         Iterator<SwallowMessage> iterator= messages.iterator();
         while(iterator.hasNext()){
            SwallowMessage msg = iterator.next();
            if(!messageFilter.getParam().contains(msg.getType())){
               iterator.remove();
            }
         }
      }

      if (LOG.isDebugEnabled()) {
         LOG.debug("fetched messages from mongodb, size:" + messages.size());
         LOG.debug("messages:" + messages);
      }
      return messages;
   }

   @Override
   public void setFetchSize(int fetchSize) {
      this.fetchSize = fetchSize;
   }

}

package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message2.SwallowMessage;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class TopicDAOImpl implements TopicDAO<Long> {

   @SuppressWarnings("unused")
   private static final Logger LOG = LoggerFactory.getLogger(TopicDAOImpl.class);

   private final DB            db;

   public TopicDAOImpl(DB db) {
      this.db = db;
   }

   /**
    * 记录的格式如：<br>
    */
   @Override
   public List<SwallowMessage> getMessagesGreaterThan(String topicName, Long messageId, int size) {
      DBCollection collection = this.db.getCollection(topicName);

      BasicDBObject keys = new BasicDBObject();
      keys.put("_id", 0);
      DBObject gt = BasicDBObjectBuilder.start().add("$gt", BSONTimestampUtils.longToBSONTimestamp(messageId)).get();
      DBObject query = BasicDBObjectBuilder.start().add("messageId", gt).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("messageId", Integer.valueOf(1)).get();
      DBCursor cursor = collection.find(query, keys).sort(orderBy).limit(size);

      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      while (cursor.hasNext()) {
         DBObject result = cursor.next();
         SwallowMessage swallowMessage = new SwallowMessage();
         BSONTimestamp timestamp = (BSONTimestamp) result.get("messageId");
         swallowMessage.setMessageId(BSONTimestampUtils.BSONTimestampToLong(timestamp));
         swallowMessage.setContent((String) result.get("content"));
         swallowMessage.setVersion((String) result.get("version"));
         swallowMessage.setGeneratedTime((Date) result.get("generatedTime"));
         String propertiesJsonStr = (String) result.get("properties");
         if (propertiesJsonStr != null) {
            JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
            swallowMessage.setProperties(jsonBinder.fromJson(propertiesJsonStr, Properties.class));
         }
         swallowMessage.setRetryCount((Integer) result.get("retryCount"));
         swallowMessage.setSha1((String) result.get("sha1"));
         list.add(swallowMessage);
      }

      return list;
   }

   @Override
   public List<SwallowMessage> getMinMessages(String topicName, int size) {
      DBCollection collection = this.db.getCollection(topicName);

      DBObject orderBy = BasicDBObjectBuilder.start().add("messageId", Integer.valueOf(1)).get();
      BasicDBObject keys = new BasicDBObject();
      keys.put("_id", 0);
      DBCursor cursor = collection.find(new BasicDBObject(), keys).sort(orderBy).limit(size);

      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      while (cursor.hasNext()) {
         DBObject result = cursor.next();
         SwallowMessage swallowMessage = new SwallowMessage();
         BSONTimestamp timestamp = (BSONTimestamp) result.get("messageId");
         swallowMessage.setMessageId(BSONTimestampUtils.BSONTimestampToLong(timestamp));
         swallowMessage.setContent((String) result.get("content"));
         swallowMessage.setVersion((String) result.get("version"));
         swallowMessage.setGeneratedTime((Date) result.get("generatedTime"));
         String propertiesJsonStr = (String) result.get("properties");
         if (propertiesJsonStr != null) {
            JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
            swallowMessage.setProperties(jsonBinder.fromJson(propertiesJsonStr, Properties.class));
         }
         swallowMessage.setRetryCount((Integer) result.get("retryCount"));
         swallowMessage.setSha1((String) result.get("sha1"));
         list.add(swallowMessage);
      }

      return list;
   }

   @Override
   public void saveMessage(String topicName, SwallowMessage message) {
      DBCollection collection = this.db.getCollection(topicName);
      Properties properties = message.getProperties();
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      String propertiesJsonStr = jsonBinder.toJson(properties);
      DBObject insert = BasicDBObjectBuilder.start().add("messageId", new BSONTimestamp())
            .add("content", message.getContent()).add("content-type", message.getContentType())
            .add("generatedTime", message.getGeneratedTime()).add("retryCount", message.getRetryCount())
            .add("version", message.getVersion()).add("properties", propertiesJsonStr).get();
      collection.insert(insert, WriteConcern.SAFE);
   }

}

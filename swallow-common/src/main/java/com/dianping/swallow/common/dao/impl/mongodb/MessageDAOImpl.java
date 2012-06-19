package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.message.JsonBinder;
import com.dianping.swallow.common.message.SwallowMessage;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;

public class MessageDAOImpl implements MessageDAO<Long> {

   @SuppressWarnings("unused")
   private static final Logger LOG = LoggerFactory.getLogger(MessageDAOImpl.class);

   private MongoClient         mongoClient;

   public void setMongoClient(MongoClient mongoClient) {
      this.mongoClient = mongoClient;
   }

   @Override
   public Long getMaxMessageId(String topicName) {

      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(-1)).get();
      DBCursor cursor = collection.find().sort(orderBy).limit(1);
      if (cursor != null) {
         BSONTimestamp timestamp = (BSONTimestamp) cursor.next().get("_id");
         return BSONTimestampUtils.BSONTimestampToLong(timestamp);
      }
      return null;
   }

   /**
    * 记录的格式如：<br>
    */
   @Override
   public List<SwallowMessage> getMessagesGreaterThan(String topicName, Long messageId, int size) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject gt = BasicDBObjectBuilder.start().add("$gt", BSONTimestampUtils.longToBSONTimestamp(messageId)).get();
      DBObject query = BasicDBObjectBuilder.start().add("_id", gt).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(1)).get();
      DBCursor cursor = collection.find(query).sort(orderBy).limit(size);

      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      while (cursor.hasNext()) {
         DBObject result = cursor.next();
         SwallowMessage swallowMessage = new SwallowMessage();
         BSONTimestamp timestamp = (BSONTimestamp) result.get("_id");
         swallowMessage.setMessageId(BSONTimestampUtils.BSONTimestampToLong(timestamp));
         swallowMessage.setContent((String) result.get("content"));
         swallowMessage.setVersion((String) result.get("version"));
         swallowMessage.setGeneratedTime((Date) result.get("generatedTime"));
         String propertiesJsonStr = (String) result.get("properties");
         if (propertiesJsonStr != null) {
            JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
            swallowMessage.getProperties().putAll(jsonBinder.fromJson(propertiesJsonStr, Properties.class));
         }
         swallowMessage.setSha1((String) result.get("sha1"));
         list.add(swallowMessage);
      }

      return list;
   }

   @Override
   public List<SwallowMessage> getMinMessages(String topicName, int size) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(1)).get();
      DBCursor cursor = collection.find().sort(orderBy).limit(size);

      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      while (cursor.hasNext()) {
         DBObject result = cursor.next();
         SwallowMessage swallowMessage = new SwallowMessage();
         BSONTimestamp timestamp = (BSONTimestamp) result.get("_id");
         swallowMessage.setMessageId(BSONTimestampUtils.BSONTimestampToLong(timestamp));
         swallowMessage.setContent((String) result.get("content"));
         swallowMessage.setVersion((String) result.get("version"));
         swallowMessage.setGeneratedTime((Date) result.get("generatedTime"));
         String propertiesJsonStr = (String) result.get("properties");
         if (propertiesJsonStr != null) {
            JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
            swallowMessage.getProperties().putAll(jsonBinder.fromJson(propertiesJsonStr, Properties.class));
         }
         swallowMessage.setSha1((String) result.get("sha1"));
         list.add(swallowMessage);
      }

      return list;
   }

   @Override
   public void saveMessage(String topicName, SwallowMessage message) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      Properties properties = message.getProperties();
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      String propertiesJsonStr = jsonBinder.toJson(properties);
      DBObject insert = BasicDBObjectBuilder.start().add("_id", new BSONTimestamp())
            .add("content", message.getContent()).add("generatedTime", message.getGeneratedTime())
            .add("version", message.getVersion()).add("properties", propertiesJsonStr).get();
      collection.insert(insert, WriteConcern.SAFE);
   }

}

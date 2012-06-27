package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class MessageDAOImpl implements MessageDAO {

   @SuppressWarnings("unused")
   private static final Logger LOG            = LoggerFactory.getLogger(MessageDAOImpl.class);

   public static final String  CONTENT        = "c";
   public static final String  VERSION        = "v";
   public static final String  SHA1           = "s";
   public static final String  GENERATED_TIME = "gt";
   public static final String  PROPERTIES     = "p";
   public static final String  TYPE           = "t";

   private MongoClient         mongoClient;

   public void setMongoClient(MongoClient mongoClient) {
      this.mongoClient = mongoClient;
   }

   @Override
   public SwallowMessage getMessage(String topicName, Long messageId) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject query = BasicDBObjectBuilder.start().add("_id", MongoUtils.longToBSONTimestamp(messageId)).get();
      DBObject result = collection.findOne(query);
      if (result != null) {
         SwallowMessage swallowMessage = new SwallowMessage();
         convert(result, swallowMessage);
         return swallowMessage;
      }
      return null;
   }

   @Override
   public Long getMaxMessageId(String topicName) {

      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject fields = BasicDBObjectBuilder.start().add("_id", 1).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(-1)).get();
      DBCursor cursor = collection.find(null, fields).sort(orderBy).limit(1);
      try {
         if (cursor.hasNext()) {
            BSONTimestamp timestamp = (BSONTimestamp) cursor.next().get("_id");
            return MongoUtils.BSONTimestampToLong(timestamp);
         }
      } finally {
         cursor.close();
      }
      return null;
   }

   @Override
   public SwallowMessage getMaxMessage(String topicName) {

      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(-1)).get();
      DBCursor cursor = collection.find().sort(orderBy).limit(1);
      try {
         if (cursor.hasNext()) {
            DBObject result = cursor.next();
            SwallowMessage swallowMessage = new SwallowMessage();
            convert(result, swallowMessage);
            return swallowMessage;
         }
      } finally {
         cursor.close();
      }
      return null;
   }

   @Override
   public List<SwallowMessage> getMessagesGreaterThan(String topicName, Long messageId, int size) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject gt = BasicDBObjectBuilder.start().add("$gt", MongoUtils.longToBSONTimestamp(messageId)).get();
      DBObject query = BasicDBObjectBuilder.start().add("_id", gt).get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(1)).get();
      DBCursor cursor = collection.find(query).sort(orderBy).limit(size);

      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      try {
         while (cursor.hasNext()) {
            DBObject result = cursor.next();
            SwallowMessage swallowMessage = new SwallowMessage();
            convert(result, swallowMessage);
            list.add(swallowMessage);
         }
      } finally {
         cursor.close();
      }
      return list;
   }

   @Override
   public List<SwallowMessage> getMessagesGreaterThan(String topicName, Long messageId, Set<String> messageTypeSet,
                                                      int size) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      DBObject gt = BasicDBObjectBuilder.start().add("$gt", MongoUtils.longToBSONTimestamp(messageId)).get();
      BasicDBObjectBuilder queryBuilder = BasicDBObjectBuilder.start().add("_id", gt);
      //TODO 取回消息再过滤
      if (!messageTypeSet.isEmpty()) {
         DBObject in = BasicDBObjectBuilder.start().add("$in", messageTypeSet).get();
         queryBuilder.add(TYPE, in);
      }
      DBObject query = queryBuilder.get();
      DBObject orderBy = BasicDBObjectBuilder.start().add("_id", Integer.valueOf(1)).get();
      DBCursor cursor = collection.find(query).sort(orderBy).limit(size);

      List<SwallowMessage> list = new ArrayList<SwallowMessage>();
      try {
         while (cursor.hasNext()) {
            DBObject result = cursor.next();
            SwallowMessage swallowMessage = new SwallowMessage();
            convert(result, swallowMessage);
            list.add(swallowMessage);
         }
      } finally {
         cursor.close();
      }
      return list;
   }

   @SuppressWarnings("unchecked")
   private void convert(DBObject result, SwallowMessage swallowMessage) {
      BSONTimestamp timestamp = (BSONTimestamp) result.get("_id");
      swallowMessage.setMessageId(MongoUtils.BSONTimestampToLong(timestamp));
      swallowMessage.setContent((String) result.get(CONTENT));//content
      swallowMessage.setVersion((String) result.get(VERSION));//version
      swallowMessage.setGeneratedTime((Date) result.get(GENERATED_TIME));//generatedTime
      String propertiesJsonStr = (String) result.get(PROPERTIES);//properties
      if (propertiesJsonStr != null) {
         JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
         swallowMessage.setProperties(jsonBinder.fromJson(propertiesJsonStr, HashMap.class));
      }
      swallowMessage.setSha1((String) result.get(SHA1));//sha1
      swallowMessage.setType((String) result.get(TYPE));//type
   }

   @Override
   public void saveMessage(String topicName, SwallowMessage message) {
      DBCollection collection = this.mongoClient.getMessageCollection(topicName);

      Map<String, String> properties = message.getProperties();
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      //TODO 直接存properties就可以
      String propertiesJsonStr = jsonBinder.toJson(properties);
      DBObject insert = BasicDBObjectBuilder.start().add("_id", new BSONTimestamp()).add(CONTENT, message.getContent())
            .add(GENERATED_TIME, message.getGeneratedTime()).add(VERSION, message.getVersion())
            .add(PROPERTIES, propertiesJsonStr).add(SHA1, message.getSha1()).add(TYPE, message.getType()).get();
      collection.insert(insert);
   }

}

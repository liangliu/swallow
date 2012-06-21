package com.dianping.swallow.common.dao.impl.mongodb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class MongoClient {

   private static final Logger LOG                     = LoggerFactory.getLogger(MongoClient.class);

   private static final String MONGO_CONFIG_FILENAME   = "swallow-mongo.properties";

   private static final String DEFAULT_COLLECTION_NAME = "c";

   private Map<String, Mongo>  topicnameToMongoMap;
   private MongoConfig         config;

   private Mongo               heartBeatMongo;

   /**
    * 从 Lion(配置topicName,serverUrl的列表) 和 MongoConfigManager(配置Mongo参数) 获取配置，创建
    * “topicName -&gt; Mongo实例” 的Map映射。<br>
    * <br>
    * 当 Lion 配置发现变化时，“topicName -&gt; Mongo实例” 的Map映射;<br>
    * 将 MongoClient 实例注入到DAO：dao通过调用MongoClient.getCo
    * 
    * @param uri
    * @param config
    */
   public MongoClient() {
      //如果存在configFile，则使用configFile
      InputStream in = MongoClient.class.getClassLoader().getResourceAsStream(MONGO_CONFIG_FILENAME);
      if (in != null) {
         this.config = new MongoConfig(in);
      } else {
         this.config = new MongoConfig();
      }

      //      try {
      //         // mongo = new Mongo(new
      //         // MongoURI("mongodb://192.168.32.111:27017,192.168.32.111:27018"));
      //         List<ServerAddress> replicaSetSeeds = parseUri(uri);
      //         mongo = new Mongo(replicaSetSeeds, getMongoOptions());
      //      } catch (Exception e) {
      //         throw new RuntimeException(e);
      //      }
      resetLionConfig();
   }

   /**
    * 读取lion配置，初始化<topicName,mongo实例>的Map<br>
    * 当lion配置发现变化时，更新Map
    */
   public void resetLionConfig() {
      //TODO 使用Lion进行初始化
      HashMap<String, Mongo> map = new HashMap<String, Mongo>();
      String uri = "mongodb://localhost:27017";
      List<ServerAddress> replicaSetSeeds = parseUri(uri);
      Mongo mongo = new Mongo(replicaSetSeeds, getMongoOptions());
      map.put("topicForUnitTest", mongo);

      this.topicnameToMongoMap = map;
      this.heartBeatMongo = mongo;
   }

   private MongoOptions getMongoOptions() {
      MongoOptions options = new MongoOptions();
      options.slaveOk = config.isSlaveOk();
      options.socketKeepAlive = config.isSocketKeepAlive();
      options.socketTimeout = config.getSocketTimeout();
      options.connectionsPerHost = config.getConnectionsPerHost();
      options.threadsAllowedToBlockForConnectionMultiplier = config.getThreadsAllowedToBlockForConnectionMultiplier();
      options.w = config.getW();
      options.wtimeout = config.getWtimeout();
      options.fsync = config.isFsync();
      options.connectTimeout = config.getConnectTimeout();
      options.maxWaitTime = config.getMaxWaitTime();
      options.autoConnectRetry = config.isAutoConnectRetry();
      options.safe = config.isSafe();
      return options;
   }

   public DBCollection getMessageCollection(String topicId) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicnameToMongoMap.get(topicId);
      if (mongo == null) {
         throw new IllegalArgumentException("topicname '" + topicId
               + "' do not match any Mongo Server, please check your config on Lion.");
      }
      return this.getCollection(mongo, "msg_", topicId);
   }

   public DBCollection getAckCollection(String topicId) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicnameToMongoMap.get(topicId);
      if (mongo == null) {
         throw new IllegalArgumentException("topicname '" + topicId
               + "' do not match any Mongo Server, please check your config on Lion.");
      }
      return this.getCollection(mongo, "ack_", topicId);
   }

   public DBCollection getHeartbeatCollection(String ip) {
      return this.getCollection(this.heartBeatMongo, "heartbeat_", ip);
   }

   private DBCollection getCollection(Mongo mongo, String dbNamePrefix, String collectionName) {
      //根据topicname从Mongo实例从获取DB
      String dbName = dbNamePrefix + collectionName;
      DB db = mongo.getDB(dbName);
      //从DB实例获取Collection(因为只有一个Collection，所以名字均叫做c),如果不存在，则创建)
      DBCollection collection = null;
      if (!db.collectionExists(DEFAULT_COLLECTION_NAME)) {
         synchronized (dbName.intern()) {
            if (!db.collectionExists(DEFAULT_COLLECTION_NAME)) {
               collection = createColletcion(db, DEFAULT_COLLECTION_NAME);
            }
         }
         if (collection == null)
            collection = db.getCollection(DEFAULT_COLLECTION_NAME);
      } else {
         collection = db.getCollection(DEFAULT_COLLECTION_NAME);
      }
      return collection;
   }

   private DBCollection createColletcion(DB db, String collectionName) {
      DBObject options = new BasicDBObject();
      options.put("capped", true);
      options.put("size", config.getCappedCollectionSize());//max db file size in bytes
      int cappedCollectionMaxDocNum = config.getCappedCollectionMaxDocNum();
      if (cappedCollectionMaxDocNum > 0) {
         options.put("max", config.getCappedCollectionMaxDocNum());//max row count
      }
      try {
         return db.createCollection(collectionName, options);
      } catch (MongoException e) {
         if (e.getMessage() != null && e.getMessage().indexOf("collection already exists") >= 0) {
            //collection already exists
            LOG.error(e.getMessage() + ":" + collectionName);
            return db.getCollection(collectionName);
         } else {
            //other exception, can not connect to mongo etc, should abort
            throw e;
         }
      }
   }

   //   public DBCollection getMessageCollection(String topicname) {
   //      return this.getCollection(this.config.getMessageDBName(), topicname);
   //   }
   //
   //   public DBCollection getAckCollection(String topicname) {
   //      return this.getCollection(this.config.getAckDBName(), topicname);
   //   }
   //
   //   public DBCollection getHeartbeatCollection(String topicname) {
   //      return this.getCollection(this.config.getHeartbeatDBName(), topicname);
   //   }

   private static List<ServerAddress> parseUri(String uri) {
      uri = uri.trim();
      String schema = "mongodb://";
      if (uri.startsWith(schema)) { // 兼容老各式uri
         uri = uri.substring(schema.length());
      }
      String[] hostPortArr = uri.split(",");
      List<ServerAddress> result = new ArrayList<ServerAddress>();
      for (int i = 0; i < hostPortArr.length; i++) {
         String[] pair = hostPortArr[i].split(":");
         try {
            result.add(new ServerAddress(pair[0].trim(), Integer.parseInt(pair[1].trim())));
         } catch (Exception e) {
            throw new IllegalArgumentException("Bad format of mongo uri", e);
         }
      }
      return result;
   }

}

package com.dianping.swallow.common.dao.impl.mongodb;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;
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
   private static final String TOPICNAME_HEARTBEAT     = "heartbeat";
   private static final String TOPICNAME_DEFAULT       = "default";
   private static final String LION_KEY_MONGO_URI      = "swallow.mongo.ServerURI";

   private Map<String, Mongo>  topicnameToMongoMap;
   private MongoOptions        mongoOptions;
   private MongoConfig         config;

   /**
    * 从 Lion(配置topicName,serverUrl的列表) 和 MongoConfigManager(配置Mongo参数) 获取配置，创建
    * “topicName -&gt; Mongo实例” 的Map映射。<br>
    * <br>
    * 当 Lion 配置发现变化时，“topicName -&gt; Mongo实例” 的Map映射;<br>
    * 将 MongoClient 实例注入到DAO：dao通过调用MongoClient.getCo
    * 
    * @param uri
    * @param config
    * @throws LionException
    */
   public MongoClient() throws LionException {
      //读取properties配置(如果存在configFile，则使用configFile)
      InputStream in = MongoClient.class.getClassLoader().getResourceAsStream(MONGO_CONFIG_FILENAME);
      if (in != null) {
         config = new MongoConfig(in);
      } else {
         config = new MongoConfig();
      }
      mongoOptions = this.getMongoOptions(config);
      //读取Lion配置
      ConfigCache cc = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
      String topicURI = cc.getProperty(LION_KEY_MONGO_URI);
      //构造Mongo实例
      this.topicnameToMongoMap = parseTopicURI(topicURI);
      //设置Lion事件响应
      cc.addChange(new ConfigChange() {
         @Override
         public void onChange(String key, String value) {
            try {
               if (LION_KEY_MONGO_URI.equals(key)) {
                  MongoClient.this.topicnameToMongoMap = parseTopicURI(value);
               }
            } catch (Exception e) {
               LOG.error("Error occour when reset config from Lion:" + e.getMessage(), e);
            }
         }
      });

   }

   /**
    * <pre>
    * <MongoURI>=<heartbeat>,<topicName>,<topicName>;<MongoURI>=<topicName>
    * </pre>
    */
   private Map<String, Mongo> parseTopicURI(String topicURI) {
      Map<String, Mongo> map = new HashMap<String, Mongo>();
      for (String uriToTopicName : topicURI.split(";")) {
         String[] splits = uriToTopicName.split("=");
         String mongoURI = splits[0];
         List<ServerAddress> replicaSetSeeds = parseUri(mongoURI);
         Mongo mongo = null;
         if (this.topicnameToMongoMap != null) {//如果已有的map中已经存在该Mongo实例，则重复使用
            for (Mongo m : this.topicnameToMongoMap.values()) {
               if (equalsOutOfOrder(m.getAllAddress(), replicaSetSeeds)) {
                  mongo = m;
                  break;
               }
            }
         }
         if (mongo == null) {//创建mongo实例
            mongo = new Mongo(replicaSetSeeds, this.mongoOptions);
         }
         String topicNameStr = splits[1];
         for (String topicName : topicNameStr.split(",")) {
            map.put(topicName, mongo);
         }
      }
      //default是必须存在的topicName
      if (!map.containsKey(TOPICNAME_DEFAULT)) {
         throw new IllegalArgumentException("The swallow.mongo.topicURI value must contain 'default' topicName!");
      }
      return map;
   }

   @SuppressWarnings({ "rawtypes", "unchecked" })
   private boolean equalsOutOfOrder(List list1, List list2) {
      if (list1 == null || list2 == null) {
         return false;
      }
      return list1.containsAll(list2) && list2.containsAll(list1);
   }

   private MongoOptions getMongoOptions(MongoConfig config) {
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

   public DBCollection getMessageCollection(String topicName) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicnameToMongoMap.get(topicName);
      if (mongo == null) {
         if (LOG.isInfoEnabled()) {
            LOG.info("topicname '" + topicName + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicnameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, "msg_", topicName);
   }

   public DBCollection getAckCollection(String topicName) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicnameToMongoMap.get(topicName);
      if (mongo == null) {
         if (LOG.isInfoEnabled()) {
            LOG.info("topicname '" + topicName + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicnameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, "ack_", topicName);
   }

   public DBCollection getHeartbeatCollection(String ip) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicnameToMongoMap.get(TOPICNAME_HEARTBEAT);
      if (mongo == null) {
         if (LOG.isInfoEnabled()) {
            LOG.info("topicname '" + TOPICNAME_HEARTBEAT + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicnameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, "heartbeat_", ip);
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

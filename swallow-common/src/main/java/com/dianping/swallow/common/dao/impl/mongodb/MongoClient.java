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

//TODO 将server的客户端连接和topic情况，连接的mongo地址，通过cat或hawk反映出来。
public class MongoClient {

   private static final Logger         LOG                     = LoggerFactory.getLogger(MongoClient.class);

   private static final String         MONGO_CONFIG_FILENAME   = "swallow-mongo.properties";
   private static final String         DEFAULT_COLLECTION_NAME = "c";
   private static final String         TOPICNAME_HEARTBEAT     = "heartbeat";
   private static final String         TOPICNAME_DEFAULT       = "default";

   private static final String         LION_KEY_MONGO_URI      = "swallow.mongo.serverURI";

   private String                      mongoServerURI;

   private volatile Map<String, Mongo> topicnameToMongoMap;
   private MongoOptions                mongoOptions;
   private MongoConfig                 config;

   /**
    * 从 Lion(配置topicName,serverUrl的列表) 和 MongoConfigManager(配置Mongo参数) 获取配置，创建
    * “topicName -&gt; Mongo实例” 的Map映射。<br>
    * <br>
    * 当 Lion 配置发现变化时，“topicName -&gt; Mongo实例” 的Map映射;<br>
    * 将 MongoClient 实例注入到DAO：dao通过调用MongoClient.getXXCollectiond得到Collection。
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
   }

   public void init() throws LionException {
      //如果mongoServerURI不为null，说明是通过Spring中配置了mongoServerURI,即使用了Lion-spring模式，故不需要手动调用Lion<br>
      //否则，如果mongoServerURI为null，则需要使用Lion-api模式。
      if (this.mongoServerURI == null) {
         ConfigCache cc = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
         this.mongoServerURI = cc.getProperty(LION_KEY_MONGO_URI);
         //构造Mongo实例
         this.topicnameToMongoMap = parseTopicURI(mongoServerURI);
         if (LOG.isInfoEnabled()) {
            if (this.topicnameToMongoMap != null) {
               LOG.info("Reset config from Lion, the property 'topicnameToMongoMap' is changed.");
            } else {
               LOG.info("init config from Lion, the property 'topicnameToMongoMap' is inited.");
            }
         }
         //设置Lion事件响应
         cc.addChange(new ConfigChange() {
            @Override
            public void onChange(String key, String value) {
               try {
                  if (LION_KEY_MONGO_URI.equals(key)) {
                     MongoClient.this.mongoServerURI = value;
                     MongoClient.this.topicnameToMongoMap = parseTopicURI(MongoClient.this.mongoServerURI);
                  }
               } catch (Exception e) {
                  LOG.error("Error occour when reset config from Lion:" + e.getMessage(), e);
               }
            }
         });
      }

   }

   /**
    * URI格式：
    * 
    * <pre>
    * <MongoURI>=<topicName>,<topicName>;<MongoURI>=<topicName>
    * MongoURI=default;mongodb://10.1.1.1:27017=feed
    * </pre>
    */
   //TODO heartbeat使用单独的配置名称
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
      //TODO 提前检查?
      if (!map.containsKey(TOPICNAME_DEFAULT)) {
         throw new IllegalArgumentException("The '" + LION_KEY_MONGO_URI
               + "' property must contain 'default' topicName!");
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
      //TODO 给不同类型collection和topicname提供size和max值配置
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
            LOG.error(e.getMessage() + ":the collectionName is " + collectionName);
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
            throw new IllegalArgumentException("Bad format of mongo uri：" + e.getMessage(), e);
         }
      }
      return result;
   }

   public String getMongoServerURI() {
      return mongoServerURI;
   }

   public void setMongoServerURI(String mongoServerURI) {
      this.mongoServerURI = mongoServerURI;
      try {
         Map<String, Mongo> map = parseTopicURI(mongoServerURI);
         if (LOG.isInfoEnabled()) {
            if (this.topicnameToMongoMap != null) {
               LOG.info("Reset config from Lion, the property 'topicnameToMongoMap' is changed.");
            } else {
               LOG.info("init config from Lion, the property 'topicnameToMongoMap' is inited.");
            }
         }
         this.topicnameToMongoMap = map;
      } catch (Exception e) {
         if (this.topicnameToMongoMap == null) {
            throw new IllegalArgumentException("Error occour when reset config from Lion:" + e.getMessage(), e);
         } else {
            LOG.error("Error occour when reset config from Lion:" + e.getMessage(), e);
         }
      }
   }

}

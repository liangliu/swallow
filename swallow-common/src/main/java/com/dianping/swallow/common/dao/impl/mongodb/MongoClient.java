package com.dianping.swallow.common.dao.impl.mongodb;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
public class MongoClient implements ConfigChange {

   private static final Logger         LOG                                              = LoggerFactory
                                                                                              .getLogger(MongoClient.class);

   private static final String         MONGO_CONFIG_FILENAME                            = "swallow-mongo.properties";
   private static final String         LION_CONFIG_FILENAME                             = "lion.properties";
   private static final String         DEFAULT_COLLECTION_NAME                          = "c";
   private static final String         TOPICNAME_HEARTBEAT                              = "heartbeat";
   private static final String         TOPICNAME_DEFAULT                                = "default";

   private static final String         LION_KEY_MSG_CAPPED_COLLECTION_SIZE              = "swallow.mongo.msgCappedCollectionSize";
   private static final String         LION_KEY_MSG_CAPPED_COLLECTION_MAX_DOC_NUM       = "swallow.mongo.msgCappedCollectionMaxDocNum";
   private static final String         LION_KEY_ACK_CAPPED_COLLECTION_SIZE              = "swallow.mongo.ackCappedCollectionSize";
   private static final String         LION_KEY_ACK_CAPPED_COLLECTION_MAX_DOC_NUM       = "swallow.mongo.ackCappedCollectionMaxDocNum";
   private static final String         LION_KEY_HEARTBEAT_SERVER_URI                    = "swallow.mongo.heartbeatServerURI";
   private static final String         LION_KEY_HEARTBEAT_CAPPED_COLLECTION_SIZE        = "swallow.mongo.heartbeatCappedCollectionSize";
   private static final String         LION_KEY_HEARTBEAT_CAPPED_COLLECTION_MAX_DOC_NUM = "swallow.mongo.heartbeatCappedCollectionMaxDocNum";
   //serverURI的名字可通过setter方法配置(consumer和producer在Lion上的名字是不同的)
   private String                      severURILionKey                                  = "swallow.mongo.consumerServerURI";                 //默认值

   //lion config
   private Map<String, Integer>        msgTopicNameToSizes;
   private Map<String, Integer>        msgTopicNameToMaxDocNums;
   private Map<String, Integer>        ackTopicNameToSizes;
   private Map<String, Integer>        ackTopicNameToMaxDocNums;
   private Mongo                       heartbeatMongo;
   private int                         heartbeatCappedCollectionSize;
   private int                         heartbeatCappedCollectionMaxDocNum;
   private volatile Map<String, Mongo> topicNameToMongoMap;

   //local config
   private MongoOptions                mongoOptions;

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
    * @throws IOException
    */
   public MongoClient() {
      if (LOG.isDebugEnabled()) {
         LOG.debug("MongoClient() - start.");
      }
      //读取properties配置(如果存在configFile，则使用configFile)
      InputStream in = MongoClient.class.getClassLoader().getResourceAsStream(MONGO_CONFIG_FILENAME);
      MongoConfig config;
      if (in != null) {
         config = new MongoConfig(in);
      } else {
         config = new MongoConfig();
      }
      mongoOptions = this.getMongoOptions(config);
      loadLionConfig();
      if (LOG.isDebugEnabled()) {
         LOG.debug("MongoClient() - done.");
      }
   }

   /**
    * URI格式,如：
    * 
    * <pre>
    * swallow.mongo.consumerServerURI：default,feed=mongodb://localhost:27017;topicForUnitTest=mongodb://192.168.31.178:27016 
    * swallow.mongo.producerServerURI：default,feed=mongodb://localhost:27017;topicForUnitTest=mongodb://192.168.31.178:27016 
    * swallow.mongo.msgCappedCollectionSize：default=1024;feed,topicForUnitTest=1025
    * swallow.mongo.msgCappedCollectionMaxDocNum：default=1024;feed,topicForUnitTest=1025
    * swallow.mongo.ackCappedCollectionSize：default=1024;feed,topicForUnitTest=1025
    * swallow.mongo.ackCappedCollectionMaxDocNum：default=1024;feed,topicForUnitTest=1025
    * 
    * swallow.mongo.heartbeatServerURI：mongodb://localhost:27017
    * swallow.mongo.heartbeatCappedCollectionSize=1024
    * swallow.mongo.heartbeatCappedCollectionMaxDocNum=1024
    * </pre>
    */
   private void loadLionConfig() {
      try {
         ConfigCache cc = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
         //如果本地文件存在，则使用Lion本地文件
         InputStream in = MongoClient.class.getClassLoader().getResourceAsStream(LION_CONFIG_FILENAME);
         if (in != null) {
            try {
               Properties props = new Properties();
               props.load(in);
               cc.setPts(props);
               if (LOG.isInfoEnabled()) {
                  LOG.info("Load Lion local config file :" + LION_CONFIG_FILENAME);
               }
            } finally {
               in.close();
            }
         }
         //serverURI
         this.topicNameToMongoMap = parseURIAndCreateTopicMongo(cc.getProperty(this.severURILionKey).trim());
         //msgTopicNameToSizes
         this.msgTopicNameToSizes = parseSizeOrDocNum(cc.getProperty(LION_KEY_MSG_CAPPED_COLLECTION_SIZE).trim());
         //msgTopicNameToMaxDocNums
         this.msgTopicNameToMaxDocNums = parseSizeOrDocNum(cc.getProperty(LION_KEY_MSG_CAPPED_COLLECTION_MAX_DOC_NUM)
               .trim());
         //ackTopicNameToSizes
         this.ackTopicNameToSizes = parseSizeOrDocNum(cc.getProperty(LION_KEY_ACK_CAPPED_COLLECTION_SIZE).trim());
         //ackTopicNameToMaxDocNums
         this.ackTopicNameToMaxDocNums = parseSizeOrDocNum(cc.getProperty(LION_KEY_ACK_CAPPED_COLLECTION_MAX_DOC_NUM)
               .trim());
         //heartbeat
         this.heartbeatMongo = parseURIAndCreateHeartbeatMongo(cc.getProperty(LION_KEY_HEARTBEAT_SERVER_URI).trim());
         this.heartbeatCappedCollectionSize = Integer.parseInt(cc
               .getProperty(LION_KEY_HEARTBEAT_CAPPED_COLLECTION_SIZE).trim());
         this.heartbeatCappedCollectionMaxDocNum = Integer.parseInt(cc.getProperty(
               LION_KEY_HEARTBEAT_CAPPED_COLLECTION_MAX_DOC_NUM).trim());
         //添加Lion监听
         cc.addChange(this);
      } catch (Exception e) {
         throw new IllegalArgumentException("Error Loading Config from Lion : " + e.getMessage(), e);
      }
   }

   /**
    * 解析URI，且创建heartbeat使用的Mongo实例
    */
   private Mongo parseURIAndCreateHeartbeatMongo(String serverURI) {
      Mongo mongo = null;
      List<ServerAddress> replicaSetSeeds = this.parseUriToAddressList(serverURI);
      mongo = getExistsMongo(replicaSetSeeds);
      if (mongo == null) {
         mongo = new Mongo(replicaSetSeeds, this.mongoOptions);
      }
      if (LOG.isInfoEnabled()) {
         LOG.info("parseURIAndCreateHeartbeatMongo() - parse " + serverURI + " to: " + mongo);
      }
      return mongo;
   }

   /**
    * 解析URI，且创建topic(msg和ack)使用的Mongo实例
    */
   private Map<String, Mongo> parseURIAndCreateTopicMongo(String serverURI) {
      try {
         //解析uri
         Map<String, List<String>> serverURIToTopicNames = new HashMap<String, List<String>>();
         boolean defaultExists = false;
         for (String topicNamesToURI : serverURI.split(";")) {
            String[] splits = topicNamesToURI.split("=");
            String mongoURI = splits[1];
            String topicNameStr = splits[0];
            List<String> topicNames = new ArrayList<String>();
            for (String topicName : topicNameStr.split(",")) {
               if (TOPICNAME_DEFAULT.equals(topicName)) {
                  defaultExists = true;
               }
               topicNames.add(topicName);
            }
            List<String> topicNames0 = serverURIToTopicNames.get(mongoURI);
            if (topicNames0 != null) {
               topicNames.addAll(topicNames0);
            }
            serverURIToTopicNames.put(mongoURI, topicNames);
         }
         //验证uri(default是必须存在的topicName)
         if (!defaultExists) {
            throw new IllegalArgumentException("The '" + this.severURILionKey
                  + "' property must contain 'default' topicName!");
         }
         //根据uri创建Mongo，放到Map
         HashMap<String, Mongo> topicNameToMongoMap = new HashMap<String, Mongo>();
         for (Map.Entry<String, List<String>> entry : serverURIToTopicNames.entrySet()) {
            String uri = entry.getKey();
            List<ServerAddress> replicaSetSeeds = parseUriToAddressList(uri);
            Mongo mongo = null;
            List<String> topicNames = entry.getValue();
            mongo = getExistsMongo(replicaSetSeeds);
            if (mongo == null) {//创建mongo实例
               mongo = new Mongo(replicaSetSeeds, this.mongoOptions);
            }
            for (String topicName : topicNames) {
               topicNameToMongoMap.put(topicName, mongo);
            }
         }
         if (LOG.isInfoEnabled()) {
            LOG.info("parseURIAndCreateTopicMongo() - parse " + serverURI + " to: " + topicNameToMongoMap);
         }
         return topicNameToMongoMap;
      } catch (Exception e) {
         throw new IllegalArgumentException(
               "Error parsing the '*ServerURI' property, the format is '<topicName>,default=<mongoURI>;<topicName>=<mongoURI>': "
                     + e.getMessage(), e);
      }
   }

   /**
    * 如果已有的map或heartbeatMongo中已经存在相同的地址的Mongo实例，则重复使用
    */
   private Mongo getExistsMongo(List<ServerAddress> replicaSetSeeds) {
      Mongo mongo = null;
      if (this.topicNameToMongoMap != null) {//如果已有的map中已经存在该Mongo实例，则重复使用
         for (Mongo m : this.topicNameToMongoMap.values()) {
            if (equalsOutOfOrder(m.getAllAddress(), replicaSetSeeds)) {
               mongo = m;
               break;
            }
         }
      }
      if (this.heartbeatMongo != null) {//如果已经存在该Mongo实例，则重复使用
         if (this.equalsOutOfOrder(this.heartbeatMongo.getAllAddress(), replicaSetSeeds)) {
            mongo = this.heartbeatMongo;
         }
      }
      if (mongo != null) {
         if (LOG.isInfoEnabled()) {
            LOG.info("getExistsMongo() return a exists Mongo instance : " + mongo);
         }
      }
      return mongo;
   }

   private Map<String, Integer> parseSizeOrDocNum(String sizeStr) {
      try {
         Map<String, Integer> topicNameToSizes = new HashMap<String, Integer>();
         boolean defaultExists = false;
         for (String topicNameToSize : sizeStr.split(";")) {
            String[] splits = topicNameToSize.split("=");
            String size = splits[1];
            String topicNameStr = splits[0];
            for (String topicName : topicNameStr.split(",")) {
               if (TOPICNAME_DEFAULT.equals(topicName)) {
                  defaultExists = true;
               }
               int intSize = Integer.parseInt(size);
               if (intSize <= 0) {
                  throw new IllegalArgumentException("Size or DocNum value must larger than 0 :" + sizeStr);
               }
               topicNameToSizes.put(topicName, intSize);
            }
         }
         //验证uri(default是必须存在的topicName)
         if (!defaultExists) {
            throw new IllegalArgumentException("The '" + this.severURILionKey
                  + "' property must contain 'default' topicName!");
         }
         if (LOG.isInfoEnabled()) {
            LOG.info("parseSizeOrDocNum() - parse " + sizeStr + " to: " + topicNameToSizes);
         }
         return topicNameToSizes;
      } catch (Exception e) {
         throw new IllegalArgumentException(
               "Error parsing the '*Size' or '*MaxDocNum' property, the format is like 'default=<int>;<topicName>,<topicName>=<int>': "
                     + e.getMessage(), e);
      }
   }

   /**
    * 响应Lion更新事件时:<br>
    * (1)若是URI变化，重新构造Mongo实例，替换现有的Map值；<br>
    * (2)若是size和docnum配置项变化，则仅更新变量本身， 即只后续的创建Collection操作有影响。<br>
    * <p>
    * 该方法保证：<br>
    * (1)当新的Lion配置值有异常时，不会改变现有的值；<br>
    * (2)当新的Lion配置值正确，在正常更新值后，能有效替换现有的Map和int值
    * </p>
    */
   @Override
   public void onChange(String key, String value) {
      if (LOG.isInfoEnabled()) {
         LOG.info("onChange() called.");
      }
      value = value.trim();
      try {
         if (this.severURILionKey.equals(key)) {
            this.topicNameToMongoMap = parseURIAndCreateTopicMongo(value);
         } else if (LION_KEY_MSG_CAPPED_COLLECTION_SIZE.equals(key)) {
            this.msgTopicNameToSizes = parseSizeOrDocNum(value);
         } else if (LION_KEY_MSG_CAPPED_COLLECTION_MAX_DOC_NUM.equals(key)) {
            this.msgTopicNameToMaxDocNums = parseSizeOrDocNum(value);
         } else if (LION_KEY_ACK_CAPPED_COLLECTION_SIZE.equals(key)) {
            this.ackTopicNameToSizes = parseSizeOrDocNum(value);
         } else if (LION_KEY_ACK_CAPPED_COLLECTION_MAX_DOC_NUM.equals(key)) {
            this.ackTopicNameToMaxDocNums = parseSizeOrDocNum(value);
         } else if (LION_KEY_HEARTBEAT_SERVER_URI.equals(key)) {
            this.heartbeatMongo = parseURIAndCreateHeartbeatMongo(value);
         } else if (LION_KEY_HEARTBEAT_CAPPED_COLLECTION_SIZE.equals(key)) {
            this.heartbeatCappedCollectionSize = Integer.parseInt(value);
            if (LOG.isInfoEnabled()) {
               LOG.info("parse " + value);
            }
         } else if (LION_KEY_HEARTBEAT_CAPPED_COLLECTION_MAX_DOC_NUM.equals(key)) {
            this.heartbeatCappedCollectionMaxDocNum = Integer.parseInt(value);
            if (LOG.isInfoEnabled()) {
               LOG.info("parse " + value);
            }
         }
      } catch (Exception e) {
         LOG.error("Error occour when reset config from Lion, no config property would changed :" + e.getMessage(), e);
      }
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
      Mongo mongo = this.topicNameToMongoMap.get(topicName);
      if (mongo == null) {
         if (LOG.isDebugEnabled()) {
            LOG.debug("topicname '" + topicName + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicNameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, getIntWithDefaultIfNull(msgTopicNameToSizes, topicName),
            getIntWithDefaultIfNull(msgTopicNameToMaxDocNums, topicName), "msg_", topicName);
   }

   private Integer getIntWithDefaultIfNull(Map<String, Integer> map, String key) {
      Integer i = map.get(key);
      if (i == null) {
         i = map.get(TOPICNAME_DEFAULT);
      }
      return i;
   }

   public DBCollection getAckCollection(String topicName) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicNameToMongoMap.get(topicName);
      if (mongo == null) {
         if (LOG.isDebugEnabled()) {
            LOG.debug("topicname '" + topicName + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicNameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, getIntWithDefaultIfNull(ackTopicNameToSizes, topicName),
            getIntWithDefaultIfNull(ackTopicNameToMaxDocNums, topicName), "ack_", topicName);
   }

   public DBCollection getHeartbeatCollection(String ip) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicNameToMongoMap.get(TOPICNAME_HEARTBEAT);
      if (mongo == null) {
         if (LOG.isInfoEnabled()) {
            LOG.info("topicname '" + TOPICNAME_HEARTBEAT + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicNameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, this.heartbeatCappedCollectionSize, this.heartbeatCappedCollectionMaxDocNum,
            "heartbeat_", ip);
   }

   private DBCollection getCollection(Mongo mongo, Integer size, Integer cappedCollectionMaxDocNum,
                                      String dbNamePrefix, String topicName) {
      //根据topicname从Mongo实例从获取DB
      String dbName = dbNamePrefix + topicName;
      DB db = mongo.getDB(dbName);
      //从DB实例获取Collection(因为只有一个Collection，所以名字均叫做c),如果不存在，则创建)
      DBCollection collection = null;
      if (!db.collectionExists(DEFAULT_COLLECTION_NAME)) {
         synchronized (dbName.intern()) {
            if (!db.collectionExists(DEFAULT_COLLECTION_NAME)) {
               collection = createColletcion(db, DEFAULT_COLLECTION_NAME, size, cappedCollectionMaxDocNum);
            }
         }
         if (collection == null)
            collection = db.getCollection(DEFAULT_COLLECTION_NAME);
      } else {
         collection = db.getCollection(DEFAULT_COLLECTION_NAME);
      }
      return collection;
   }

   private DBCollection createColletcion(DB db, String collectionName, Integer size, Integer cappedCollectionMaxDocNum) {
      DBObject options = new BasicDBObject();
      options.put("capped", true);
      options.put("size", size);//max db file size in bytes
      if (cappedCollectionMaxDocNum != null && cappedCollectionMaxDocNum.intValue() > 0) {
         options.put("max", cappedCollectionMaxDocNum.intValue());//max row count
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

   private List<ServerAddress> parseUriToAddressList(String uri) {
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
            throw new IllegalArgumentException(e.getMessage() + ". Bad format of mongo uri：" + uri
                  + ". The correct format is mongodb://<host>:<port>,<host>:<port>", e);
         }
      }
      return result;
   }

   public void setSeverURILionKey(String severURILionKey) {
      this.severURILionKey = severURILionKey;
   }

}

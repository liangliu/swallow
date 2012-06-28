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
public class MongoClient implements ConfigChange {

   private static final Logger         LOG                                              = LoggerFactory
                                                                                              .getLogger(MongoClient.class);

   private static final String         MONGO_CONFIG_FILENAME                            = "swallow-mongo.properties";
   private static final String         DEFAULT_COLLECTION_NAME                          = "c";
   private static final String         TOPICNAME_HEARTBEAT                              = "heartbeat";
   private static final String         TOPICNAME_DEFAULT                                = "default";

   private static final String         LION_KEY_MSG_CAPPED_COLLECTION_SIZE              = "msgCappedCollectionSize";
   private static final String         LION_KEY_MSG_CAPPED_COLLECTION_MAX_DOC_NUM       = "msgCappedCollectionMaxDocNum";
   private static final String         LION_KEY_ACK_CAPPED_COLLECTION_SIZE              = "ackCappedCollectionSize";
   private static final String         LION_KEY_ACK_CAPPED_COLLECTION_MAX_DOC_NUM       = "ackCappedCollectionMaxDocNum";
   private static final String         LION_KEY_HEARTBEAT_SERVER_URI                    = "heartbeatServerURI";
   private static final String         LION_KEY_HEARTBEAT_CAPPED_COLLECTION_SIZE        = "heartbeatCappedCollectionSize";
   private static final String         LION_KEY_HEARTBEAT_CAPPED_COLLECTION_MAX_DOC_NUM = "heartbeatCappedCollectionMaxDocNum";
   //serverURI的名字可配置(consumer和producer在Lion上的名字是不同的)
   private String                      severURILionKey;

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
    */
   public MongoClient() throws LionException {
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
    * 
    * 当URI变化时，重新构造Mongo实例；size和docnum配置项变化时，仅更新变量本身，即只后续的创建Collection操作有影响。
    * 
    * @throws LionException
    */
   private void loadLionConfig() throws LionException {
      ConfigCache cc = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
      //serverURI
      String serverURI = cc.getProperty(this.severURILionKey);
      this.topicNameToMongoMap = parseURIAndCreateTopicMongo(serverURI);
      //msgTopicNameToSizes
      String msgTopicCappedCollectionSize = cc.getProperty(LION_KEY_MSG_CAPPED_COLLECTION_SIZE);
      this.msgTopicNameToSizes = parseSizeOrDocNum(msgTopicCappedCollectionSize);
      //msgTopicNameToMaxDocNums
      String msgTopicCappedCollectionMaxDocNum = cc.getProperty(LION_KEY_MSG_CAPPED_COLLECTION_MAX_DOC_NUM);
      this.msgTopicNameToMaxDocNums = parseSizeOrDocNum(msgTopicCappedCollectionMaxDocNum);
      //ackTopicNameToSizes
      String ackTopicCappedCollectionSize = cc.getProperty(LION_KEY_ACK_CAPPED_COLLECTION_SIZE);
      this.msgTopicNameToSizes = parseSizeOrDocNum(ackTopicCappedCollectionSize);
      //ackTopicNameToMaxDocNums
      String ackTopicCappedCollectionMaxDocNum = cc.getProperty(LION_KEY_ACK_CAPPED_COLLECTION_MAX_DOC_NUM);
      this.msgTopicNameToMaxDocNums = parseSizeOrDocNum(ackTopicCappedCollectionMaxDocNum);
      //heartbeat
      List<ServerAddress> heartbeatServerAddressList = parseUriToAddressList(cc
            .getProperty(LION_KEY_HEARTBEAT_SERVER_URI));
      this.heartbeatMongo = new Mongo(heartbeatServerAddressList, this.mongoOptions);
      this.heartbeatCappedCollectionSize = Integer.parseInt(cc.getProperty(LION_KEY_HEARTBEAT_CAPPED_COLLECTION_SIZE));
      this.heartbeatCappedCollectionMaxDocNum = Integer.parseInt(cc
            .getProperty(LION_KEY_HEARTBEAT_CAPPED_COLLECTION_MAX_DOC_NUM));
   }

   private Mongo createHeartbeatMongo(String serverURI) {
      Mongo mongo = null;
      List<ServerAddress> replicaSetSeeds = this.parseUriToAddressList(serverURI);
      if (this.heartbeatMongo != null) {
         if (this.equalsOutOfOrder(this.heartbeatMongo.getAllAddress(), replicaSetSeeds)) {
            mongo = this.heartbeatMongo;
         }
      }
      if (mongo == null) {
         mongo = new Mongo(replicaSetSeeds, this.mongoOptions);
      }
      return mongo;
   }

   private Map<String, Mongo> parseURIAndCreateTopicMongo(String serverURI) {
      //解析uri
      Map<String, List<String>> serverURIToTopicNames = new HashMap<String, List<String>>();
      for (String topicNamesToURI : serverURI.split(";")) {
         String[] splits = topicNamesToURI.split("=");
         String mongoURI = splits[1];
         String topicNameStr = splits[0];
         List<String> topicNames = new ArrayList<String>();
         for (String topicName : topicNameStr.split(",")) {
            topicNames.add(topicName);
         }
         List<String> topicNames0 = serverURIToTopicNames.get(mongoURI);
         if (topicNames0 != null) {
            topicNames.addAll(topicNames0);
         }
         serverURIToTopicNames.put(mongoURI, topicNames);
      }
      //TODO 验证uri,对于输入要有完备的验证（default是必须存在的topicName）

      //根据uri创建Mongo，放到Map
      HashMap<String, Mongo> topicNameToMongoMap = new HashMap<String, Mongo>();
      for (Map.Entry<String, List<String>> entry : serverURIToTopicNames.entrySet()) {
         String uri = entry.getKey();
         List<ServerAddress> replicaSetSeeds = parseUriToAddressList(uri);
         Mongo mongo = null;
         List<String> topicNames = entry.getValue();
         if (this.topicNameToMongoMap != null) {//如果已有的map中已经存在该Mongo实例，则重复使用
            for (Mongo m : this.topicNameToMongoMap.values()) {
               if (equalsOutOfOrder(m.getAllAddress(), replicaSetSeeds)) {
                  mongo = m;
                  break;
               }
            }
         }
         if (mongo == null) {//创建mongo实例
            mongo = new Mongo(replicaSetSeeds, this.mongoOptions);
         }
         for (String topicName : topicNames) {
            topicNameToMongoMap.put(topicName, mongo);
         }
      }

      return topicNameToMongoMap;
   }

   //TODO default是必须存在的topicName
   private Map<String, Integer> parseSizeOrDocNum(String sizeStr) {
      Map<String, Integer> topicNameToSizes = new HashMap<String, Integer>();
      for (String topicNameToSize : sizeStr.split(";")) {
         String[] splits = topicNameToSize.split("=");
         String size = splits[1];
         String topicNameStr = splits[0];
         for (String topicName : topicNameStr.split(",")) {
            topicNameToSizes.put(topicName, Integer.parseInt(size));
         }
      }
      return topicNameToSizes;
   }

   //TODO 保证配置的值有异常时，不会改变现有的域；保证正常更新配置后，能有效替换Map和int值。
   @Override
   public void onChange(String key, String value) {
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
            this.heartbeatMongo = createHeartbeatMongo(value);
         } else if (LION_KEY_HEARTBEAT_CAPPED_COLLECTION_SIZE.equals(key)) {
            this.heartbeatCappedCollectionSize = Integer.parseInt(value);
         } else if (LION_KEY_HEARTBEAT_CAPPED_COLLECTION_MAX_DOC_NUM.equals(key)) {
            this.heartbeatCappedCollectionMaxDocNum = Integer.parseInt(value);
         }
      } catch (Exception e) {
         LOG.error("Error occour when reset config from Lion:" + e.getMessage(), e);
      }
   }

   //   /**
   //    * URI格式：
   //    * 
   //    * <pre>
   //    * <MongoURI>=<topicName>,<topicName>;<MongoURI>=<topicName>
   //    * 例如：
   //    * swallow.mongo.topicServerURI：swallow.mongo.serverURI=mongodb://localhost:27017=default,feed;mongodb://192.168.31.178:27016=topicForUnitTest
   //    * swallow.mongo.topicCollectionSize=1024
   //    * swallow.mongo.topicCollectionSize=1024
   //    * swallow.mongo.heartbeatServerURI：mongodb://localhost:27017
   //    * </pre>
   //    */
   //   //TODO heartbeat使用单独的配置名称
   //   private Map<String, Mongo> parseTopicURI(String topicURI) {
   //      Map<String, Mongo> map = new HashMap<String, Mongo>();
   //      for (String uriToTopicName : topicURI.split(";")) {
   //         String[] splits = uriToTopicName.split("=");
   //         String mongoURI = splits[0];
   //         List<ServerAddress> replicaSetSeeds = parseUri(mongoURI);
   //         Mongo mongo = null;
   //         if (this.topicNameToMongoMap != null) {//如果已有的map中已经存在该Mongo实例，则重复使用
   //            for (Mongo m : this.topicNameToMongoMap.values()) {
   //               if (equalsOutOfOrder(m.getAllAddress(), replicaSetSeeds)) {
   //                  mongo = m;
   //                  break;
   //               }
   //            }
   //         }
   //         if (mongo == null) {//创建mongo实例
   //            mongo = new Mongo(replicaSetSeeds, this.mongoOptions);
   //         }
   //         String topicNameStr = splits[1];
   //         for (String topicName : topicNameStr.split(",")) {
   //            map.put(topicName, mongo);
   //         }
   //      }
   //      //default是必须存在的topicName
   //      //TODO 提前检查?
   //      if (!map.containsKey(TOPICNAME_DEFAULT)) {
   //         throw new IllegalArgumentException("The '" + LION_KEY_MONGO_URI
   //               + "' property must contain 'default' topicName!");
   //      }
   //      return map;
   //   }

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
         if (LOG.isInfoEnabled()) {
            LOG.info("topicname '" + topicName + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicNameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, this.msgTopicNameToSizes.get(topicName),
            this.msgTopicNameToMaxDocNums.get(topicName), "msg_", topicName);//TODO get获取为null时使用get("default")
   }

   public DBCollection getAckCollection(String topicName) {
      //根据topicName获取Mongo实例
      Mongo mongo = this.topicNameToMongoMap.get(topicName);
      if (mongo == null) {
         if (LOG.isInfoEnabled()) {
            LOG.info("topicname '" + topicName + "' do not match any Mongo Server, use default.");
         }
         mongo = this.topicNameToMongoMap.get(TOPICNAME_DEFAULT);
      }
      return this.getCollection(mongo, this.ackTopicNameToSizes.get(topicName),
            this.ackTopicNameToMaxDocNums.get(topicName), "ack_", topicName);//TODO get获取为null时使用get("default")
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
            throw new IllegalArgumentException("Bad format of mongo uri：" + e.getMessage(), e);
         }
      }
      return result;
   }

   //   public void setMongoServerURI(String mongoServerURI) {
   //      this.mongoServerURI = mongoServerURI;
   //      try {
   //         Map<String, Mongo> map = parseTopicURI(mongoServerURI);
   //         if (LOG.isInfoEnabled()) {
   //            if (this.topicnameToMongoMap != null) {
   //               LOG.info("Reset config from Lion, the property 'topicnameToMongoMap' is changed.");
   //            } else {
   //               LOG.info("init config from Lion, the property 'topicnameToMongoMap' is inited.");
   //            }
   //         }
   //         this.topicnameToMongoMap = map;
   //      } catch (Exception e) {
   //         if (this.topicnameToMongoMap == null) {
   //            throw new IllegalArgumentException("Error occour when reset config from Lion:" + e.getMessage(), e);
   //         } else {
   //            LOG.error("Error occour when reset config from Lion:" + e.getMessage(), e);
   //         }
   //      }
   //   }

}

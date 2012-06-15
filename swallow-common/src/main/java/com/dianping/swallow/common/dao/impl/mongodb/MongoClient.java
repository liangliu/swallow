/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-7-29
 * $Id$
 * 
 * Copyright 2011 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.Mongo;
import com.mongodb.MongoOptions;
import com.mongodb.ServerAddress;

public class MongoClient {

   private static Logger log = Logger.getLogger(MongoClient.class);

   public Mongo          mongo;
   private ConfigManager config;

   //   private final static int MONGO_ORDER_ASC  = 1;
   //   private final static int MONGO_ORDER_DESC = -1;

   private final String  HEARTBEAT_DB;
   private final String  HEARTBEAT_COLLECTION;
   private final String  TOPICSEQ_DB;
   private final String  TOPICSEQ_COLLECTION;
   private final String  DURABLE_SUBSCRIBER_COUNTER_DB;
   private final String  DURABLE_SUBSCRIBER_COUNTER_COLLECTION;
   private final String  NON_DURABLE_SUBSCRIBER_COUNTER_COLLECTION;

   public MongoClient(String uri, ConfigManager config) {
      // Mongo connection which validates writes have occurred in two places
      this.config = config;
      try {
         // mongo = new Mongo(new
         // MongoURI("mongodb://192.168.32.111:27017,192.168.32.111:27018"));
         List<ServerAddress> replicaSetSeeds = MongoUtil.parseUri(uri);
         mongo = new Mongo(replicaSetSeeds, getDefaultOptions());
      } catch (Exception e) {
         throw new RuntimeException(e);
      }
      HEARTBEAT_DB = config.getHeartbeatDB();
      HEARTBEAT_COLLECTION = config.getHeartbeatCollection();
      TOPICSEQ_DB = config.getTopicSeqDB();
      TOPICSEQ_COLLECTION = config.getTopicSeqCollection();
      DURABLE_SUBSCRIBER_COUNTER_DB = config.getDurableSubscriberCounterDB();
      DURABLE_SUBSCRIBER_COUNTER_COLLECTION = config.getDurableSubscriberCounterCollection();
      NON_DURABLE_SUBSCRIBER_COUNTER_COLLECTION = config.getDurableNonSubscriberCounterCollection();
      // TODO: set write concern, if replicas_safe and 2 mongo server, all
      // request will fail when one server down!
      // mongo.setWriteConcern(WriteConcern.REPLICAS_SAFE);
   }

   private MongoOptions getDefaultOptions() {
      MongoOptions options = new MongoOptions();
      options.slaveOk = config.isMongoSlaveOk();
      options.socketKeepAlive = config.isMongoSocketKeepAlive();
      options.socketTimeout = config.getMongoSocketTimeout();
      options.connectionsPerHost = config.getMongoConnectionsPerHost();
      options.threadsAllowedToBlockForConnectionMultiplier = config
            .getMongoThreadsAllowedToBlockForConnectionMultiplier();
      options.w = config.getMongoW();
      options.wtimeout = config.getMongoWtimeout();
      options.fsync = config.isMongoFsync();
      options.connectTimeout = config.getMongoConnectTimeout();
      options.maxWaitTime = config.getMongoMaxWaitTime();
      options.autoConnectRetry = config.isMongoAutoConnectRetry();
      options.safe = config.isMongoSafe();
      return options;
   }

}

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

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * @author marsqing
 * 
 */
public class ConfigManager {

	private static Logger log = Logger.getLogger(ConfigManager.class);

	// time related
	private int pullFailDelayBase = 500;
	private int pullFailDelayUpperBound = 5000;
	private int heartbeatCheckInterval = 3000;
	// TODO: default ok?
	private int heartbeatMaxStopTime = 120000;
	private int heartbeatUpdateInterval = 10000;
	private int topicSeqMismatchSleepTime = 500;
	private int mongoFailoverSleepTime = 1000;
	private int closeSleepTime = 5000;
	private int producerSendRetryInterval = 1000;

	// db and collection name related
	private String queueDB = "swallow_queue";
	private String topicDB = "swallow_topic";
	private String heartbeatDB = "swallow_heartbeat";
	private String heartbeatCollection = "heartbeat";
	private String topicSeqDB = "swallow_topicseq";
	private String topicSeqCollection = "toicseq";
	private String durableSubscriberCounterDB = "swallow_counter";
	private String durableSubscriberCounterCollection = "subs_counter";
	private String durableNonSubscriberCounterCollection = "subs_non_counter";

	/**
	 * @return the durableNonSubscriberCounterCollection
	 */
	public String getDurableNonSubscriberCounterCollection() {
		return durableNonSubscriberCounterCollection;
	}

	// mongo server options
	private boolean mongoSlaveOk = false;
	private boolean mongoSocketKeepAlive = false;
	private int mongoSocketTimeout = 500;
	private int mongoConnectionsPerHost = 30;
	private int mongoThreadsAllowedToBlockForConnectionMultiplier = 50;
	private int mongoW = 0;
	private int mongoWtimeout = 500;
	private boolean mongoFsync = false;
	private int mongoConnectTimeout = 500;
	private int mongoMaxWaitTime = 1000 * 60 * 2;
	private boolean mongoAutoConnectRetry = false;
	private boolean mongoSafe = false;

	public static void main(String[] args) {
		new ConfigManager();
	}

	public ConfigManager() {
		this("swallow.properties");
	}

	public ConfigManager(String configFileName) {
		InputStream in = ConfigManager.class.getClassLoader().getResourceAsStream(configFileName);
		Properties props = new Properties();
		Class clazz = this.getClass();
		if (in != null) {
			try {
				props.load(in);
				in.close();
				for (String key : props.stringPropertyNames()) {
					Field field = null;
					try {
						field = clazz.getDeclaredField(key.trim());
					} catch (Exception e) {
						log.error("unknow property found in " + configFileName + ": " + key);
						continue;
					}
					field.setAccessible(true);
					if (field.getType().equals(Integer.TYPE)) {
						try {
							field.set(this, Integer.parseInt(props.getProperty(key).trim()));
						} catch (Exception e) {
							log.error("cat not parse property " + key, e);
							continue;
						}
					} else if (field.getType().equals(Long.TYPE)) {
						try {
							field.set(this, Long.parseLong(props.getProperty(key).trim()));
						} catch (Exception e) {
							log.error("cat not set property " + key, e);
							continue;
						}
					} else if (field.getType().equals(String.class)) {
						try {
							field.set(this, props.getProperty(key).trim());
						} catch (Exception e) {
							log.error("cat not set property " + key, e);
							continue;
						}
					} else {
						try {
							field.set(this, Boolean.parseBoolean(props.getProperty(key).trim()));
						} catch (Exception e) {
							log.error("cat not set property " + key, e);
							continue;
						}
					}
				}

			} catch (IOException e) {
				log.error("Error reading " + configFileName, e);
			}
		} else {
			log.info(configFileName + " not found, use default");
		}
		if (log.isDebugEnabled()) {
			Field[] fields = clazz.getDeclaredFields();
			for (int i = 0; i < fields.length; i++) {
				Field f = fields[i];
				f.setAccessible(true);
				if (!Modifier.isStatic(f.getModifiers())) {
					try {
						log.debug(f.getName() + "=" + f.get(this));
					} catch (Exception e) {
					}
				}
			}
		}
	}

	/***
	 * 
	 * @return mongo中暂时没有消息时等待的时间基数，每次失败后等待的时间为pullFailDelayBase*连续失败次数
	 */
	public int getPullFailDelayBase() {
		return pullFailDelayBase;
	}

	/***
	 * 
	 * @return mongo中暂时没有消息时等待的时间上限
	 */
	public int getPullFailDelayUpperBound() {
		return pullFailDelayUpperBound;
	}

	/***
	 * 
	 * @return slave consumer检查master consumer心跳的时间间隔
	 */
	public int getHeartbeatCheckInterval() {
		return heartbeatCheckInterval;
	}

	/***
	 * 
	 * @return master consumer心跳最长的停止时间
	 */
	public int getHeartbeatMaxStopTime() {
		return heartbeatMaxStopTime;
	}

	/***
	 * 
	 * @return master consumer更新心跳的间隔
	 */
	public int getHeartbeatUpdateInterval() {
		return heartbeatUpdateInterval;
	}

	/***
	 * 
	 * @return topic消息seq发生乱序时等待正确seq消息的时间，只等待一次
	 */
	public int getTopicSeqMismatchSleepTime() {
		return topicSeqMismatchSleepTime;
	}

	/***
	 * 
	 * @return mongo master挂掉时mongo操作的重试时间间隔
	 */
	public int getMongoFailoverSleepTime() {
		return mongoFailoverSleepTime;
	}

	/***
	 * 
	 * @return 调用MQService.close()时等待consumer完成当前处理的sleep的时间
	 */
	public int getCloseSleepTime() {
		return closeSleepTime;
	}

	/**
	 * 
	 * @return producer消息重发的时间间隔
	 */
	public int getProducerSendRetryInterval() {
		return producerSendRetryInterval;
	}

	public boolean isMongoSlaveOk() {
		return mongoSlaveOk;
	}

	public boolean isMongoSocketKeepAlive() {
		return mongoSocketKeepAlive;
	}

	public int getMongoSocketTimeout() {
		return mongoSocketTimeout;
	}

	public int getMongoConnectionsPerHost() {
		return mongoConnectionsPerHost;
	}

	public int getMongoThreadsAllowedToBlockForConnectionMultiplier() {
		return mongoThreadsAllowedToBlockForConnectionMultiplier;
	}

	public int getMongoW() {
		return mongoW;
	}

	public int getMongoWtimeout() {
		return mongoWtimeout;
	}

	public boolean isMongoFsync() {
		return mongoFsync;
	}

	public int getMongoConnectTimeout() {
		return mongoConnectTimeout;
	}

	public int getMongoMaxWaitTime() {
		return mongoMaxWaitTime;
	}

	public boolean isMongoAutoConnectRetry() {
		return mongoAutoConnectRetry;
	}

	public boolean isMongoSafe() {
		return mongoSafe;
	}

	public String getQueueDB() {
		return queueDB;
	}

	public String getTopicDB() {
		return topicDB;
	}

	public String getHeartbeatDB() {
		return heartbeatDB;
	}

	public String getHeartbeatCollection() {
		return heartbeatCollection;
	}

	public String getDurableSubscriberCounterDB() {
		return durableSubscriberCounterDB;
	}

	public String getDurableSubscriberCounterCollection() {
		return durableSubscriberCounterCollection;
	}

	public String getTopicSeqDB() {
		return topicSeqDB;
	}

	public String getTopicSeqCollection() {
		return topicSeqCollection;
	}
	
}

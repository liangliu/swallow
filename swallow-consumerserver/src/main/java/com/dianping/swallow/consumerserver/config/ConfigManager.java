package com.dianping.swallow.consumerserver.config;


import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * @author zhang.yu
 * 
 */
public class ConfigManager {
	
	private static Logger log = Logger.getLogger(ConfigManager.class);
	
	private static ConfigManager ins = new ConfigManager();

	private int freeChannelBlockQueueSize = 10;
	// time related
	private int heartbeatCheckInterval = 3000;
	private int heartbeatMaxStopTime = 120000;
	private int heartbeatUpdateInterval = 10000;
	private int blockQueueFailoverSleepTime = 1000;
	private int pullingTime = 1000;
	private long freeChannelBlockQueueOutTime = 120000;

	// db and collection name related
	private String counterDB = "swallow_counter";
	private String topicDB = "swallow_topic";
	
	//Master Ip
	private String masterIp = "127.0.0.1";
	

	public int getBlockQueueFailoverSleepTime() {
		return blockQueueFailoverSleepTime;
	}


	public String getMasterIp() {
		return masterIp;
	}


	public int getHeartbeatCheckInterval() {
		return heartbeatCheckInterval;
	}


	public long getFreeChannelBlockQueueOutTime() {
		return freeChannelBlockQueueOutTime;
	}


	public int getFreeChannelBlockQueueSize() {
		return freeChannelBlockQueueSize;
	}


	public int getPullingTime() {
		return pullingTime;
	}

	public String getCounterDB() {
		return counterDB;
	}

	public static void main(String[] args) {
		new ConfigManager();
	}
	
	public static ConfigManager getInstance() {
		return ins;
	}

	private ConfigManager() {
		this("swallow.properties");
	}

	private ConfigManager(String configFileName) {
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


	public String getTopicDB() {
		return topicDB;
	}
}

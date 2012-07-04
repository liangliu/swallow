package com.dianping.swallow.common.config.impl.lion;

import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.dianping.lion.EnvZooKeeperConfig;
import com.dianping.lion.client.ConfigCache;
import com.dianping.lion.client.ConfigChange;
import com.dianping.lion.client.LionException;
import com.dianping.swallow.common.config.ConfigChangeListener;
import com.dianping.swallow.common.config.DynamicConfig;
import com.dianping.swallow.common.dao.impl.mongodb.MongoClient;

public class LionDynamicConfig implements DynamicConfig {

	private static final Logger LOG = LoggerFactory.getLogger(LionDynamicConfig.class);

	private ConfigCache cc;
	
	public LionDynamicConfig(String localConfigFileName) {
		try {
			cc = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
			// 如果本地文件存在，则使用Lion本地文件
			InputStream in = MongoClient.class.getClassLoader().getResourceAsStream(localConfigFileName);
			if (in != null) {
				try {
					Properties props = new Properties();
					props.load(in);
					cc.setPts(props);
					if (LOG.isInfoEnabled()) {
						LOG.info("Load Lion local config file :" + localConfigFileName);
					}
				} finally {
					in.close();
				}
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String get(String key) {
		try {
			return cc.getProperty(key);
		} catch (LionException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void setConfigChangeListener(final ConfigChangeListener listener) {
		cc.addChange(new ConfigChange() {
			
			@Override
			public void onChange(String key, String value) {
				listener.onConfigChange(key, value);
			}
		});
	}

}

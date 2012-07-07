package com.dianping.swallow.common.config;

public interface DynamicConfig {

   String get(String key);

   void setConfigChangeListener(ConfigChangeListener listener);
}

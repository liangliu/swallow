package com.dianping.swallow.common.internal.config;

public interface DynamicConfig {

   String get(String key);

   void setConfigChangeListener(ConfigChangeListener listener);
}

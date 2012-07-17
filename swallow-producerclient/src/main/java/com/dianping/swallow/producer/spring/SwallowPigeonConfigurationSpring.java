package com.dianping.swallow.producer.spring;

import com.dianping.swallow.producer.impl.internal.SwallowPigeonConfiguration;

public class SwallowPigeonConfigurationSpring extends SwallowPigeonConfiguration {
   public void init() {
      checkHostsAndWeights();
      checkSerialize();
      checkTimeout();
      checkUseLion();
   }

   public void setHosts(String hosts) {
      this.hosts = hosts;
   }

   public void setWeights(String weights) {
      this.weights = weights;
   }
}

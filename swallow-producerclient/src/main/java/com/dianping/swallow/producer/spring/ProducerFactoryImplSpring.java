package com.dianping.swallow.producer.spring;

import com.dianping.swallow.common.producer.exceptions.RemoteServiceInitFailedException;
import com.dianping.swallow.producer.impl.ProducerFactoryImpl;

public class ProducerFactoryImplSpring extends ProducerFactoryImpl{
   public void init() throws RemoteServiceInitFailedException {
      super.remoteServiceTimeout = getPigeonConfigure().getTimeout();
      super.remoteService = super.initPigeon(getPigeonConfigure());
   }

}

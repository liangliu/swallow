package com.dianping.swallow.common.internal.util;

import net.sf.cglib.proxy.Enhancer;

import com.mongodb.MongoException;

public class ProxyUtil {
   private ProxyUtil() {
   }

   /**
    * 返回一个代理类，代理了所有targetClass的方法，代理后的方法实现了以下功能：<br>
    * 在方法抛出MongoException异常类型(或其子类)时，会不断重试，直到不抛出异常为止。
    * 
    * @param targetClass 被代理的类的Class
    * @param retryIntervalWhenException 异常发生时，睡眠retryIntervalWhenException后才重新尝试
    * @param 指定异常的Class
    * @return 返回代理的targetClass
    */
   @SuppressWarnings("unchecked")
   public static <T> T createMongoDaoProxyWithRetryMechanism(T targetClass, long retryIntervalWhenException) {
      Enhancer enhancer = new Enhancer();
      enhancer.setSuperclass(targetClass.getClass());
      enhancer.setCallback(new RetryMethodInterceptor(targetClass, retryIntervalWhenException, MongoException.class));
      return (T) enhancer.create();
   }

}

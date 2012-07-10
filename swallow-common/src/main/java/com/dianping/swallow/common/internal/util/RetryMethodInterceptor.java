package com.dianping.swallow.common.internal.util;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 代理后的方法实现了以下功能：<br>
 * 在方法抛出异常时，会不断重试，直到不抛出异常为止。
 * 
 * @author wukezhu
 */
class RetryMethodInterceptor implements MethodInterceptor {
   private static final Logger LOG = LoggerFactory.getLogger(RetryMethodInterceptor.class);
   private long                retryIntervalWhenException;
   private Object              target;

   public RetryMethodInterceptor(Object target, long retryIntervalWhenException) {
      this.target = target;
      this.retryIntervalWhenException = retryIntervalWhenException;
   }

   @Override
   public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
      while (true) {
         try {
            return proxy.invoke(this.target, args);
         } catch (Exception e) {
            LOG.error(
                  "Error in Proxy of " + this.target + ", wait " + retryIntervalWhenException + "ms before retry. ", e);
            Thread.sleep(retryIntervalWhenException);
         }
      }
   }

}

package com.dianping.swallow.common.internal.util;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.MethodInterceptor;
import net.sf.cglib.proxy.MethodProxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;

/**
 * 代理后的方法实现了以下功能：<br>
 * 在方法抛出异常时，会不断重试，直到不抛出异常为止。
 * 
 * @author wukezhu
 */
@SuppressWarnings("rawtypes")
class RetryMethodInterceptor implements MethodInterceptor {
   private static final Logger LOG = LoggerFactory.getLogger(RetryMethodInterceptor.class);
   private long                retryIntervalWhenException;
   private Object              target;
   /** 指定异常的Class */
   private Class               clazz;

   public RetryMethodInterceptor(Object target, long retryIntervalWhenException, Class clazz) {
      this.target = target;
      this.retryIntervalWhenException = retryIntervalWhenException;
      this.clazz = clazz;
   }

   @Override
   public Object intercept(Object obj, Method method, Object[] args, MethodProxy proxy) throws Throwable {
      while (true) {
         try {
            return proxy.invoke(this.target, args);
         } catch (Exception e) {
            //判断异常类型,如果是指定的异常或异常子类，则retry
            if (clazz.isInstance(e)) {
               LOG.error("Error in Proxy of " + this.target + ", wait " + retryIntervalWhenException
                     + "ms before retry. ", e);
               Thread.sleep(retryIntervalWhenException);
            } else {
               throw e;
            }
         }
      }
   }

   public static void main(String[] args) {
      MongoException obj = new MongoException("");
      System.out.println(RuntimeException.class.isInstance(obj));
   }

}

package com.dianping.swallow.common.producer.exceptions;

/**
 * 调用Producer的SendMessage函数时，如果远程调用服务不可用（可能是网络异常或配置错误等原因），则抛出该异常
 * 
 * @author Icloud
 */
public class RemoteServiceDownException extends RuntimeException {
   private static final long serialVersionUID = -8885826779834945921L;

   @Override
   public String getMessage() {
      return "Remote service's status is unusual.";
   }
}

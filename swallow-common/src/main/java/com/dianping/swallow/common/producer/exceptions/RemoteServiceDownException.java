package com.dianping.swallow.common.producer.exceptions;

/**
 * 调用Producer的SendMessage函数时，如果远程调用服务不可用（可能是网络异常或配置错误等原因），则抛出该异常
 * 
 * @author tong.song
 */
public class RemoteServiceDownException extends RuntimeException {
   private static final long serialVersionUID = -8885826779834945921L;

   public RemoteServiceDownException(String message, Throwable cause) {
      super(message, cause);
   }

   public RemoteServiceDownException(Throwable cause) {
      super("Remote service's status is unusual.", cause);
   }
}

package com.dianping.swallow.common.producer.exceptions;

/**
 * 调用Producer的SendMessage函数时，如果传入的Content为空，则抛出此异常
 * 
 * @author tong.song
 */
//TODO 修改构造方法
public class NullContentException extends RuntimeException {
   private static final long serialVersionUID = -6251131303134970375L;

   @Override
   public String getMessage() {
      return "Content can not be null.";
   }

}

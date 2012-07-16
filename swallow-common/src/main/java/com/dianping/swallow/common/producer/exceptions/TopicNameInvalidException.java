package com.dianping.swallow.common.producer.exceptions;

/**
 * 目的地名称非法时抛出的异常，目的地名称只能由字母,数字,小数点“.”,减号“-”和下划线“_”构成，只能以字母开头，长度为2到30
 * 
 * @author tong.song
 */
public class TopicNameInvalidException extends Exception {

   private static final long serialVersionUID = 5536528063495154789L;

   @Override
   public String getMessage() {
      return "Topic name is invalid.";
   }

}

/**
 * Project: swallow-common
 * 
 * File Created at 2012-6-27
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.common.internal.util;

public class NameCheckUtil {
   private NameCheckUtil() {
   }

   /**
    * 判定topicName是否合法
    * 
    * <pre>
    * topicName由字母,数字,小数点“.”,减号“-”和下划线“_”构成，只能以字母开头，长度为2到30。
    * </pre>
    * 
    * @param topicName
    * @return 合法返回true，非法返回false
    */
   public static boolean isTopicNameValid(String topicName) {
      if (topicName == null || topicName.length() == 0) {
         return false;
      }
      if (topicName.matches("[a-z|A-Z][a-z|A-Z|_|\\-|0-9]{1,29}")) {
         return true;
      }
      return false;
   }

   /**
    * 判定consumerId是否合法
    * 
    * <pre>
    * consumerId由字母,数字,小数点“.”,减号“-”和下划线“_”构成，只能以字母开头，长度为2到30。
    * </pre>
    * 
    * @param consumerId
    * @return 合法返回true，非法返回false
    */
   public static boolean isConsumerIdValid(String consumerId) {
      if (consumerId == null || consumerId.length() == 0) {
         return false;
      }
      if (consumerId.matches("[a-z|A-Z][a-z|A-Z|_|\\-|0-9]{1,29}")) {
         return true;
      }
      return false;
   }

   public static void main(String[] args) {
      System.out.println(isTopicNameValid("ab"));
      System.out.println(isTopicNameValid("a_"));
      System.out.println(isTopicNameValid("a1"));
      System.out.println(isTopicNameValid("ab_"));
      System.out.println(isTopicNameValid("a1-0"));
      System.out.println(isTopicNameValid("a1234567890123456789"));
      System.out.println(isTopicNameValid("a1."));
      System.out.println(isTopicNameValid("a"));//false
      System.out.println(isTopicNameValid("_"));//false
      System.out.println(isTopicNameValid("3"));//false
      System.out.println(isTopicNameValid("a123456789012345678901234567890"));//false
   }
}

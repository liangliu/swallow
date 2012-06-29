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
package com.dianping.swallow.common.producer;

/**
 * 给Producer用的工具类
 * 
 * @author tong.song
 */
public class ProducerUtils {
   /**
    * 判定topicName是否合法
    * @param topicName
    * @return 合法返回true，非法返回false
    */
   public static boolean isTopicNameValid(String topicName) {
      if (topicName.matches("[a-z | A-Z | _ | . | 0-9]+"))
         return true;
      return false;
   }
}

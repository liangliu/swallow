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
 * TODO Comment of ProducerUtils
 * 
 * @author tong.song
 */
public class ProducerUtils {
   public static boolean isTopicNameValid(String topicName) {
      if (topicName == null)
         return false;
      if (topicName == "")
         return false;
      return true;
   }
}

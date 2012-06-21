/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-21
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
package com.dianping.swallow.producer;

import java.util.Map;

/**
 * TODO Comment of Producer
 * 
 * @author tong.song
 */
public interface Producer {
   public String sendMessage(Object content);

   public String sendMessage(Object content, String messageType);

   public String sendMessage(Object content, Map<String, String> properties);

   public String sendMessage(Object content, Map<String, String> properties, String messageType);
}

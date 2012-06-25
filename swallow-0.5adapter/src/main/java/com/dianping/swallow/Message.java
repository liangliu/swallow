/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-7-29
 * $Id$
 * 
 * Copyright 2011 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow;

import java.util.Date;
import java.util.Set;

public interface Message {

	// void ack();
	/**
	 * 消息发送时间
	 */
	Date getAddtime();

	/**
	 * 
	 * @return 消息内容
	 */
	Object getContent();
	
	/**
	 * 
	 * @param name
	 * @param value
	 */
	void setProperty(String name, String value);
	
	String getProperty(String name);
	
	Set<String> getPropertyNames();
	
	boolean isBackout();
	int getBackoutCnt();
	String getBackoutReason();
	/**
	 * @return 原始消息的发送时间
	 */
	Date getBackoutOriginalAddtime();
	String getBackoutStackTrace();

}

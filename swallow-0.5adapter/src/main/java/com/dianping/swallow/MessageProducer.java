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

public interface MessageProducer {

	/**
	 * 发送消息
	 * 
	 * @param msg
	 * @throws MQException
	 *             如果访问mongo出现错误，且超过了指定的重试次数
	 */
	void send(Message msg) throws MQException;

	/**
	 * 创建文本消息
	 * 
	 * @param content
	 * @return
	 */
	StringMessage createStringMessage(String content);

	/**
	 * 创建binary消息
	 * 
	 * @param content
	 * @return
	 */
	BinaryMessage createBinaryMessage(byte[] content);
	
	/**
	 * 设置无法投递消息的handler
	 * @param handler
	 */
	void setUndeliverableMessageHandler(UndeliverableMessageHandler handler);

}

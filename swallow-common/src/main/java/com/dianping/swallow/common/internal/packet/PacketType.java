/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-25
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
package com.dianping.swallow.common.internal.packet;

/**
 * 通信报文种类
 * @author tong.song
 *
 */
public enum PacketType {
   /**
    * Producer向Swallow发送的greet报文
    */
	PRODUCER_GREET,
	/**
	 * Swallow向Producer发送的消息保存成功确认报文
	 */
	SWALLOW_P_ACK,
	/**
	 * Consumer向Swallow发送的收到推送消息确认报文
	 */
	CONSUMER_ACK,
	/**
	 * Consumer向Swallow发送的greet报文
	 */
	CONSUMER_GREET,
	/**
	 * 承载SwallowMessage对象的消息报文
	 */
	OBJECT_MSG,
}

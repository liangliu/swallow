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

import java.io.Serializable;

/**
 * Producer、Consumer与Swallow通信的报文抽象类
 * @author tong.song
 *
 */
public abstract class Packet implements Serializable{
   private static final long serialVersionUID = 3871861235495404121L;
   private PacketType packetType;
	public void setPacketType(PacketType packetType){
		this.packetType = packetType;
	}
	public PacketType getPacketType(){
		return packetType;
	}
}

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
package com.dianping.swallow.common.packet;

import java.io.Serializable;

/**
 * TODO Comment of Packet
 * @author tong.song
 *
 */
public abstract class Packet implements Serializable{
	private PacketType packetType;
	public void setPacketType(PacketType packetType){
		this.packetType = packetType;
	}
	public PacketType getPacketType(){
		return packetType;
	}
}

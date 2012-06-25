/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-28
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

import com.dianping.swallow.common.message.Destination;

/**
 * TODO Comment of PktMessage
 * @author tong.song
 *
 */
public final class PktTextMessage extends Packet{
	private PktObjectMessage	objMsg;
	private boolean				isACK;

	public PktTextMessage(PktObjectMessage objMsg, boolean isACK) {
		this.setPacketType(PacketType.TEXT_MSG);
		
		this.objMsg	= objMsg;
		this.isACK	= isACK;
	}
	
	public boolean isACK() {
		return isACK;
	}

	public PktObjectMessage getObjMsg() {
		return objMsg;
	}
}
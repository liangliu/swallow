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

import com.dianping.swallow.common.util.Destination;


/**
 * TODO Comment of PktBinaryMessage
 * @author tong.song
 *
 */
public final class PktBinaryMessage extends Packet implements Message{
	private Destination dest;
	private byte[] content;
	
	public PktBinaryMessage(Destination dest, byte[] content){
		this.setPacketType(PacketType.BINARY_MSG);
		this.dest = dest;
		this.setContent(content);
	}
	public void setContent(byte[] content) {
		this.content = content;
	}
	@Override
	public byte[] getContent() {
		return content;
	}
	@Override
	public Destination getDestination() {
		// TODO Auto-generated method stub
		return dest;
	}
}

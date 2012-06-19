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

import java.util.Date;

import com.dianping.swallow.common.util.Destination;

/**
 * TODO Comment of PktMessage
 * @author tong.song
 *
 */
public final class PktTextMessage extends Packet implements Message{
	private Destination		dest;
	private String			content;
	private Date			date;
	private boolean			isACK;

	public PktTextMessage(Destination dest, String content, boolean isACK) {
		this.setPacketType(PacketType.TEXT_MSG);
		
		this.setDest(dest);
		this.setContent(content);
		this.date = new Date();
		this.setACK(isACK);
	}
	
	@Override
	public String getContent() {
		return content;
	}
	
	@Override
	public Destination getDestination() {
		return dest;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return getContent();
	}
	//Getters && Setters
	public Date getDate() {
		return date;
	}

	public Destination getDest() {
		return dest;
	}

	public void setDest(Destination dest) {
		this.dest = dest;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public boolean isACK() {
		return isACK;
	}

	public void setACK(boolean isACK) {
		this.isACK = isACK;
	}
}
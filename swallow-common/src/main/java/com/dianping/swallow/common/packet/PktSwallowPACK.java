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

/**
 * TODO Comment of PktSwallowPACK
 * @author tong.song
 *
 */
public final class PktSwallowPACK extends Packet {
	private String shaInfo;

	public PktSwallowPACK(String shaInfo){
		this.setPacketType(PacketType.SWALLOW_P_ACK);
		this.setShaInfo(shaInfo);
	}
	public String getShaInfo() {
		return shaInfo;
	}
	public void setShaInfo(String shaInfo) {
		this.shaInfo = shaInfo;
	}
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return shaInfo;
	}
}

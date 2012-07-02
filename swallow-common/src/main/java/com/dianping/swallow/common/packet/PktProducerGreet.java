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
 * Producer向Swallow发送的Greet报文，包含Producer的版本号
 * @author tong.song
 *
 */
public final class PktProducerGreet extends Packet {
   private static final long serialVersionUID = 3904928996496374432L;
   private String version;
	private String producerIP;
	public PktProducerGreet(){
	   
	}
	public PktProducerGreet(String producerVersion, String producerIP){
		this.setPacketType(PacketType.PRODUCER_GREET);
		this.setProducerVersion(producerVersion);
		this.setProducerIP(producerIP);
	}
	
	private void setProducerVersion(String producerVersion) {
		this.version = producerVersion;
	}
	public String getProducerVersion() {
		return version;
	}
   public String getProducerIP() {
      return producerIP;
   }
   public void setProducerIP(String producerIP) {
      this.producerIP = producerIP;
   }
}
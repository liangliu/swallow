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

import com.dianping.swallow.common.message.SwallowMessage;

/**
 * TODO Comment of PktMessage
 * 
 * @author tong.song
 */
@SuppressWarnings("serial")
public final class PktTextMessage extends Packet {
   private SwallowMessage message;
   private boolean        isACK;
   private String         topicName;

   public PktTextMessage(String topicName, SwallowMessage message, boolean isACK) {
      this.setPacketType(PacketType.TEXT_MSG);
      this.topicName = topicName;
      this.message = message;
      this.isACK = isACK;
   }

   public boolean isACK() {
      return isACK;
   }

   public SwallowMessage getMessage() {
      return message;
   }

   public String getTopicName() {
      return topicName;
   }


}

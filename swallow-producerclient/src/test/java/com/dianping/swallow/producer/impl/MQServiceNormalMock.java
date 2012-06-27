/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-27
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
package com.dianping.swallow.producer.impl;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.packet.PktSwallowPACK;
import com.dianping.swallow.common.producer.MQService;

/**
 * TODO Comment of MQServiceNormalMock
 * 
 * @author tong.song
 */
public class MQServiceNormalMock implements MQService {

   /*
    * (non-Javadoc)
    * @see
    * com.dianping.swallow.common.producer.MQService#sendMessage(com.dianping
    * .swallow.common.packet.Packet)
    */
   @Override
   public Packet sendMessage(Packet pkt) throws Exception {
      return new PktSwallowPACK("This is a mock ACK.");
   }

}

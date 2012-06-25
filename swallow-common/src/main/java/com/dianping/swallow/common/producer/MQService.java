package com.dianping.swallow.common.producer;

import com.dianping.swallow.common.packet.Packet;

public interface MQService {
   public Packet sendMessage(Packet pkt) throws Exception;
}

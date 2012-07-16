package com.dianping.swallow.producer;

import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.producer.exceptions.SendFailedException;

public interface ProducerHandler {
   public Packet doSendMsg(Packet pkt) throws SendFailedException;
}

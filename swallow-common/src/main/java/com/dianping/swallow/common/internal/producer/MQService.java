package com.dianping.swallow.common.internal.producer;

import com.dianping.swallow.common.internal.packet.Packet;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

public interface MQService {
   public Packet sendMessage(Packet pkt) throws ServerDaoException;
}

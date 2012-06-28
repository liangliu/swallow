package com.dianping.swallow.common.producer;

import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.producer.exceptions.ServerDaoException;

public interface MQService {
   public Packet sendMessage(Packet pkt) throws ServerDaoException;
}

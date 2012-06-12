package com.dianping.swallow.common.util;

import com.dianping.swallow.common.message.TextMessage;
import com.dianping.swallow.common.packet.Packet;

public interface MQService {
	public Packet sendMessage(Packet pkt);
}

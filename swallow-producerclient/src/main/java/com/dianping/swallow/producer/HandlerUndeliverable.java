package com.dianping.swallow.producer;

import com.dianping.swallow.common.packet.Message;
import com.dianping.swallow.common.packet.Packet;
import com.dianping.swallow.common.util.Destination;

public interface HandlerUndeliverable {
	void handleUndeliverableMessage(Message msg);
}

package com.dianping.swallow.producer;

import com.dianping.swallow.common.packet.Message;

public interface HandlerUndeliverable {
	void handleUndeliverableMessage(Message msg);
}

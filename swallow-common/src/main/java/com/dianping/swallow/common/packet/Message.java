package com.dianping.swallow.common.packet;

import com.dianping.swallow.common.producer.Destination;

public interface Message {
	public Object getContent();
	public Destination getDestination();
}

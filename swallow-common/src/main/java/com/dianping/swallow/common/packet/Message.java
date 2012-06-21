package com.dianping.swallow.common.packet;

import com.dianping.swallow.common.util.Destination;

public interface Message {
	public Object getContent();
	public Destination getDestination();
}

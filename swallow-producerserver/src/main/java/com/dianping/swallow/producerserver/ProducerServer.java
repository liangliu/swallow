package com.dianping.swallow.producerserver;

import com.dianping.swallow.common.message.TextMessage;

public interface ProducerServer {
	public String getStr(TextMessage strMsg);
}

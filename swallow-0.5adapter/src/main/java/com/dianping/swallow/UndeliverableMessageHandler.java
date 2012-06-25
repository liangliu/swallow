package com.dianping.swallow;

/**
 * 处理Producer无法发送的消息
 * @author qing.gu
 *
 */
public interface UndeliverableMessageHandler {

	void handleUndeliverableMessage(Destination dest, Message msg);
	
}

package com.dianping.swallow.consumer;

import org.jboss.netty.channel.MessageEvent;



public interface MessageListener {

	/**
	 * 消息处理回调方法
	 * 
	 * @param msg
	 */
	void onMessage(MessageEvent e);

}

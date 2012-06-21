package com.dianping.swallow.consumer;



public interface MessageListener {

	/**
	 * 消息处理回调方法
	 * 
	 * @param msg
	 */
	void onMessage(String msg);

}

package com.dianping.swallow.consumer;

import com.dianping.swallow.common.message.SwallowMessage;

public interface MessageListener {

   /**
    * 消息处理回调方法
    * 
    * @param msg
    */
   void onMessage(SwallowMessage msg);

}

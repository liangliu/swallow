package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.SwallowMessage;



public class TestConsumer {
	
	//应该都是取自于lion
	public static String swallowCAddress = "1.1.1.1:8000,1.1.1.2:8000";
	public static String cid = "zhangyu";
	public static Destination dest = Destination.topic("zhangyu");
	public static ConsumerType consumerType = ConsumerType.UPDATE_BEFORE_ACK;
    public static void main(String[] args) throws Exception {

    	//TODO 通过spring使用的example
    	ConsumerClient cClient = new ConsumerClient(cid, dest, swallowCAddress);
    	cClient.setConsumerType(consumerType);
    	cClient.setListener(new MessageListener() {

			@Override
			public void onMessage(SwallowMessage msg) {
				//用户得到SwallowMessage，
				System.out.println(msg.getContent());
			}

		});
    	cClient.beginConnect();
    	
    }
}
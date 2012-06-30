package com.dianping.swallow.consumer;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.message.SwallowMessage;



public class TestConsumer {
	
	//应该都是取自于lion
	public static String swallowCAddress = "127.0.0.1:8081,127.0.0.1:8082";
	public static String cid = "zhangyu";
	public static Destination dest = Destination.topic("xx");
	public static ConsumerType consumerType = ConsumerType.AT_LEAST;
    public static void main(String[] args) throws Exception {

    	//TODO 通过spring使用的example
    	ConsumerClient cClient = new ConsumerClient(cid, dest, swallowCAddress);
    	cClient.setConsumerType(consumerType);
    	cClient.setListener(new MessageListener() {

			@Override
			public void onMessage(SwallowMessage msg) {
				//用户得到SwallowMessage，
				System.out.println(msg.getMessageId() + ":" + msg.getContent());
				try {
					Thread.sleep(500);
				} catch (InterruptedException e) {
					// TODO 使用LOG
					e.printStackTrace();
				}
			}

		});
    	cClient.beginConnect();
    	
    }
}
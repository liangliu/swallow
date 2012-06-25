package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;

import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.util.Destination;



public class TestConsumer {
	//应该都是取自于lion
	public static String host = "127.0.0.1";
//	public static String host = "10.1.14.79";
	public static int marsterPort = 8081;
	public static int slavePort = 8082;
	public static String cid = "zhangyu";
	public static Destination dest;
    public static void main(String[] args) throws Exception {
    	
    	
    	//System.out.println(Integer.MAX_VALUE / (3600*24*365));
    	
    	
    	ConsumerClient cClient = new ConsumerClient(cid, dest);  
    	cClient.setListener(new MessageListener() {

			@Override
			public void onMessage(SwallowMessage msg) {				
				System.out.println(msg.getContent());
			}

		});
    	cClient.beginConnect(new InetSocketAddress(host, marsterPort));
    	
    }
}
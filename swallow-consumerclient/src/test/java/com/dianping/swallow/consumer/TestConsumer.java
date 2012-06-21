package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;


public class TestConsumer {
	//应该都是取自于lion
	public static String host = "127.0.0.1";
//	public static String host = "10.1.14.79";
	public static int marsterPort = 8081;
	public static int slavePort = 8082;
    public static void main(String[] args) throws Exception {
    	
    	ConsumerClient cClient = new ConsumerClient();  
    	cClient.setListener(new MessageListener() {

			@Override
			public void onMessage(String msg) {				
				System.out.println(msg);
			}

		});
    	cClient.beginConnect(new InetSocketAddress(host, marsterPort));
    	
    }
}
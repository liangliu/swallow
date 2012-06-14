package com.dianping.swallow.producer.impl;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.dianping.filequeue.DefaultFileQueueImpl;
import com.dianping.filequeue.FileQueue;
import com.dianping.filequeue.FileQueueClosedException;
import com.dianping.swallow.common.packet.Packet;

public class HandlerAsynchroMode {
	private Producer			producer;
	private ExecutorService		senders; 				//filequeue处理线程池
	private FileQueue<Packet>	messageQueue; 			//filequeue

	public HandlerAsynchroMode(Producer producer){
		this.producer	= producer;
		messageQueue	= new DefaultFileQueueImpl<Packet>("filequeue.properties", producer.getProducerID());//filequeue
		senders			= Executors.newFixedThreadPool(Producer.SENDER_NUM);
		this.start();
	}
	//对外的接口
	public void doSendMsg(Packet pkt) throws FileQueueClosedException{
		messageQueue.add(pkt);
	}
	
	private void start(){
		int idx;
		for(idx = 0; idx < Producer.SENDER_NUM; idx++){
			senders.execute(new TskGetAndSend());
		}
	}
	private class TskGetAndSend implements Runnable{
		@Override
		public void run() {
			while(true){
				//如果filequeue无元素则阻塞，否则发送
				producer.getSwallowAgency().sendMessage(messageQueue.get());
			}
		}
	}
}

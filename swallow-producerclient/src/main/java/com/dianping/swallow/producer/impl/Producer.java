/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-24
 * $Id$
 * 
 * Copyright 2010 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.producer.impl;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;

import com.dianping.swallow.producer.Destination;

/**
 * TODO Comment of Producer
 * @author tong.song
 *
 */
public class Producer {
	private Map<InetSocketAddress, Integer> 		swallowPList		= new HashMap<InetSocketAddress, Integer>();
	private Map<InetSocketAddress, ChannelFuture>	channelFutureList	= new HashMap<InetSocketAddress, ChannelFuture>();
	private Map<InetSocketAddress, Integer>			punishList			= new HashMap<InetSocketAddress, Integer>();
	private InetSocketAddress	currentSwallowP		= null;
	private Destination			dest				= null;
	private int					ackNum				= 0;
	private int					retryTimes			= 10;
	private final int			SEND_TIMEOUT		= 3000;
	private final int			TIMEOUT_VARIANCE	= (int)(SEND_TIMEOUT * 0.1);
	private final int			PUNISH_TIMES		= 10;
	
	public InetSocketAddress getCurrentSwallowP() {
		return currentSwallowP;
	}
	public int getAckNum(){
		return ackNum;
	}
	
	//构造函数
	public Producer(int destType, String destName, int retryTimes){
		this.retryTimes = retryTimes;
		for(int i = 0; i < 2; i++){
			swallowPList.put(new InetSocketAddress("127.0.0.1", 8080+i), (i+1)*10);
		}
		switch(destType){
		case 0://queue
			dest = Destination.queue(destName);
			break;
		case 1://topic
			dest = Destination.queue(destName);
			break;
		default:
			break;
		}
		connectToSwallowP();
	}
	
	//Lion配置更新
	private void onPropertyChanged(){
		//更新swallowPList
		//清空punishList
		punishList.clear();
		//检查channelFutureList
		Iterator channelFutureListIter = channelFutureList.entrySet().iterator();
		while(channelFutureListIter.hasNext()){
			Map.Entry entry = (Map.Entry)channelFutureListIter.next();
			if(!swallowPList.containsKey(entry.getKey())){
				((ChannelFuture)entry.getValue()).getChannel().close();
				channelFutureListIter.remove();
			}
		}
		//检查swallowPList
		Iterator swallowPListIter = swallowPList.entrySet().iterator();
		while(swallowPListIter.hasNext()){
			Map.Entry entry = (Map.Entry)swallowPListIter.next();
			if(!channelFutureList.containsKey(entry.getKey())){
				channelFutureList.put((InetSocketAddress)entry.getKey(), getChannelFuture((InetSocketAddress)entry.getKey()));
			}
		}
	}
	
	//获得与指定SwallowP连接的ChannelFuture
	public ChannelFuture getChannelFuture(InetSocketAddress swallowPAddr) {
		ClientBootstrap bootstrap = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));
		bootstrap.setPipelineFactory(new ProducerChannelPipelineFactory(this));
		ChannelFuture future = bootstrap.connect(swallowPAddr);
		return future;
	}
	
	//连接所有SwallowP
	public void connectToSwallowP(){
		Iterator swallowPIter = swallowPList.entrySet().iterator();
		//建立全连接
		while(swallowPIter.hasNext()){
			Map.Entry entry = (Map.Entry)swallowPIter.next();
			channelFutureList.put(
					(InetSocketAddress)entry.getKey(), 
					getChannelFuture((InetSocketAddress)entry.getKey())
					);
		}
	}
	
	//发送StringMessage
	public void sendStringMessage(String content){
		PkgStringMessage stringMsg = new PkgStringMessage(dest, content, ++ ackNum);
		doSendMessage(stringMsg, retryTimes);
	}
	
	//发送BinaryMessage
	public void sendBinaryMessage(byte[] content){
		PkgBinaryMessage binaryMsg = new PkgBinaryMessage(dest, content, ++ ackNum);
		doSendMessage(binaryMsg, retryTimes);
	}
	
	//实际发送Message
	private synchronized void doSendMessage(Package Message, int retryTimes){
		final InetSocketAddress addr = chooseSwallowP();
		if(addr == null && retryTimes > 0){//无可用swallowP
			System.out.println("No available swallowP. Resend message for " + (this.retryTimes - retryTimes + 1) + " times.");
			doSendMessage(Message, --retryTimes);
			return;
		}
		this.currentSwallowP = addr;
		if((ChannelFuture)channelFutureList.get(addr) != null){
			System.out.println("got right future");
		}else{
			System.out.println("(ChannelFuture)channelFutureList.get(addr) is null!");
			return;
		}
		((ChannelFuture)channelFutureList.get(addr)).getChannel().write(Message);
		//超时处理
		long startSendTime = System.currentTimeMillis();
		try {
			wait(SEND_TIMEOUT);
		} catch (InterruptedException e) {
			//处理wait异常
		}
		long endSendTime = System.currentTimeMillis();
		if(endSendTime - startSendTime >= SEND_TIMEOUT - TIMEOUT_VARIANCE){//wait过程中没有收到notifyAll，未收到ACK
			System.out.println("No ack from server: " + addr + ", punish server for " + PUNISH_TIMES + " times.");
			punishList.put(addr, PUNISH_TIMES);
			if(retryTimes > 0){//需要重发
				System.out.println("Wait for: " + (endSendTime - startSendTime) + ", no response. Resend message for " + (this.retryTimes - retryTimes + 1) + " times.");
				doSendMessage(Message, --retryTimes);
			}
		}
		else{
			System.out.println("Get right ack, message is sent successfully. with time spent: " + (endSendTime - startSendTime));
		}
	}

	//根据权重返回SwallowP地址//返回null：无可用swallowP
	public InetSocketAddress chooseSwallowP(){
		//构造临时swallowP列表//获取到当前连接通畅、未被惩罚、权重不为零的swallowP地址
		InetSocketAddress	addrRet				= null;
		Map					tempSwallowPList	= new HashMap<InetSocketAddress, Integer>();
		Iterator			swallowPListIter	= swallowPList.entrySet().iterator();
		int					sumWeight			= 0;
		while(swallowPListIter.hasNext()){
			Map.Entry entry = (Map.Entry)swallowPListIter.next();
			if(punishList.containsKey(entry.getKey()) //在惩罚列表中
					|| entry.getValue() == Integer.valueOf(0) //权重为零
					|| !channelFutureList.containsKey(entry.getKey()) //未建立连接
					|| !((ChannelFuture)channelFutureList.get(entry.getKey())).getChannel().isConnected()){//连接断开
				continue;
			}
			tempSwallowPList.put(entry.getKey(), entry.getValue());
			sumWeight += (Integer)entry.getValue();
		}
		System.out.println("tempSwallowPList's size is: " + tempSwallowPList.size());
		//选择swallowP
		if(tempSwallowPList.size() > 0){
			int randSeed = new Random().nextInt(sumWeight);
			int curWeight = 0;
			Iterator tempIter = tempSwallowPList.entrySet().iterator();
			while(tempIter.hasNext()){
				Map.Entry entry = (Map.Entry)tempIter.next();
				curWeight += (Integer)entry.getValue();
				if(randSeed <= curWeight){
					addrRet = (InetSocketAddress)entry.getKey();
					break;
				}
			}
		}
		//更新惩罚列表//每次新选择swallowP就更新惩罚列表
		renewPunishList();
		return addrRet;
	}

	//更新punishList//惩罚次数-1，等于0则移除
	private void renewPunishList(){
		if(punishList.isEmpty()) return;
		Iterator punishListIter = punishList.entrySet().iterator();
		while(punishListIter.hasNext()){
			Map.Entry entry = (Map.Entry)punishListIter.next();
			if((Integer)entry.getValue()-1>0){
				punishList.put((InetSocketAddress)entry.getKey(), (Integer)entry.getValue()-1);
			}else{
				punishListIter.remove();
			}
		}
	}

	//测试函数
	public static void main(String[] args){
		Producer producer = new Producer(0, "master.slave", 10);
		try{
			Thread.sleep(2000);
		}catch(Exception e){
			
		}
		for(int i = 0; i < 1000; i ++){
			long begin = System.currentTimeMillis();
			producer.sendStringMessage("this is a string message");
			long end = System.currentTimeMillis();
			System.out.println("string message sent, spent time: " + (end - begin));
			try{
				Thread.sleep(2000);
			}catch(Exception e){
				
			}
		}
	}
}
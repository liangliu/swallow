package com.dianping.swallow.consumerserver;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ArrayBlockingQueue;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.Channel;
import com.dianping.swallow.common.dao.CounterDAO;
import com.dianping.swallow.common.dao.impl.CounterDAOImpl;

public class GetMessageThread implements Runnable{
	private String topicId;
	private String consumerId;
	private String preparedMes = null;
	private String message = null;
	private boolean isLive = true;
	CounterDAO dao = CounterDAOImpl.getCounterDAO();
	public void setTopicId(String topicId) {
		this.topicId = topicId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	@Override
	public void run() {
				

		//maxTStamp = dao.getMaxTimeStamp(topicId, consumerId);
		
		HashMap<Channel, String> channels = ConsumerService.cService.getChannelWorkStatue().get(consumerId);		
		while(isLive){
			Iterator<Entry<Channel, String>> iterator = channels.entrySet().iterator();
			while(iterator.hasNext()){
				Map.Entry<Channel, String> entry = (Entry<Channel, String>) iterator.next();
				if(entry.getValue().equals("done")){								
					//TODO 换成message的类，暂时用String代替吧。
					if(preparedMes != null){
						message = preparedMes;
						preparedMes = null;
					} else{
						//用blockingqueue就不用在内存中记录最大timeStamp了。
						ArrayBlockingQueue<String> messages = ConsumerService.cService.getMessageQueue().get(consumerId);
						try {
							String message = messages.take();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//TODO 从消息中得到maxTStamp
						BSONTimestamp maxTStamp = new BSONTimestamp();
						//更新mongo中最大timeStamp
						dao.addCounter(topicId, consumerId,maxTStamp);
					}										
					Channel channel = entry.getKey();
					//如果此时连接断了，则把消息存到预发消息变量中。
					if(!channel.isConnected()){
						preparedMes = message;
						channels.remove(channel);
						if(channels.isEmpty()){
							isLive = false;
						}
					} else{
						channel.write(message);
						channels.put(channel, "doing");
					}				
					break;
				}
			}
			try {
				//处理完一次，线程睡眠，然后继续执行。
				Thread.sleep(ConsumerService.cService.getConfigManager().getPullingTime());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
		
}

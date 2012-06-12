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
		
		//TODO refactor to ConsumerService
		HashMap<Channel, String> channels = ConsumerService.cService.getChannelWorkStatue().get(consumerId);		
		while(isLive){
			Iterator<Entry<Channel, String>> iterator = channels.entrySet().iterator();
			while(iterator.hasNext()){
				Map.Entry<Channel, String> entry = (Entry<Channel, String>) iterator.next();
				if("done".equals(entry.getValue())){								
					//TODO 换成message的类，暂时用String代替吧。
					if(preparedMes != null){
						message = preparedMes;
						preparedMes = null;
					} else{
						//用blockingqueue就不用在内存中记录最大timeStamp了。
						//TODO 放到while外可以
						ArrayBlockingQueue<String> messages = ConsumerService.cService.getMessageQueue().get(consumerId);
						try {
							String message = messages.take();
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						//TODO 从消息中得到maxTStamp
						//TODO 是否可以自己设置BSongtimestamp
						BSONTimestamp maxTStamp = new BSONTimestamp();
						//更新mongo中最大timeStamp
						//TODO 受到ack再更新
						dao.addCounter(topicId, consumerId,maxTStamp);
					}										
					Channel channel = entry.getKey();
					//如果此时连接断了，则把消息存到预发消息变量中。
					if(!channel.isConnected()){
						preparedMes = message;
						channels.remove(channel);
						if(channels.isEmpty()){
							//TODO 清理consumerID对应的Thread
							//TODO 状态用一个类统一管理
							isLive = false;
						}
					} else{
						//TODO 如何防止内存使用过量，问游泳
						channel.write(message);
						channels.put(channel, "doing");
					}
					//TODO delete break?
					break;
				}
			}
			try {
				//处理完一次，线程睡眠，然后继续执行。
				//TODO 接收到ack的时候触发发送线程工作
				Thread.sleep(ConsumerService.cService.getConfigManager().getPullingTime());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
		
}

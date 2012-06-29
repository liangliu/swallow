package com.dianping.swallow.consumerserver.worker;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.bson.types.BSONTimestamp;
import org.jboss.netty.channel.Channel;

import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.dao.AckDAO;
import com.dianping.swallow.common.dao.MessageDAO;
import com.dianping.swallow.common.dao.impl.mongodb.MongoUtils;
import com.dianping.swallow.common.message.Message;
import com.dianping.swallow.common.message.SwallowMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.common.threadfactory.MQThreadFactory;
import com.dianping.swallow.consumerserver.GetMessageThread;
import com.dianping.swallow.consumerserver.HandleACKThread;
import com.dianping.swallow.consumerserver.buffer.SwallowBuffer;

public class ConsumerWorkerImpl implements ConsumerWorker {
	
	private ConsumerInfo consumerInfo;
	private BlockingQueue<Channel> freeChannels = new ArrayBlockingQueue<Channel>(10);
	private Set<Channel> connectedChannels = new HashSet<Channel>();
	private ArrayBlockingQueue<Runnable> ackWorker = new ArrayBlockingQueue<Runnable>(10);
	private boolean getMessageThreadExist = Boolean.FALSE;
	private boolean handleACKThreadExist = Boolean.FALSE;
	private BlockingQueue<Message> messageQueue = null;
    private AckDAO ackDao;
    private SwallowBuffer swallowBuffer;
    private MessageDAO messageDao;
    private PktMessage preparedMessage = null;
    private SwallowMessage message;
    private long getMessageInterval = 1000L;
    private MQThreadFactory threadFactory;
    private String consumerid;
    private String topicName;
	public Set<Channel> getConnectedChannels() {
		return connectedChannels;
	}

	public void setGetMessageThreadExist(boolean getMessageThreadExist) {
		this.getMessageThreadExist = getMessageThreadExist;
	}

	public void setHandleACKThreadExist(boolean handleACKThreadExist) {
		this.handleACKThreadExist = handleACKThreadExist;
	}

	public ConsumerWorkerImpl(ConsumerInfo consumerInfo, AckDAO ackDao, MessageDAO messageDao, SwallowBuffer swallowBuffer, MQThreadFactory threadFactory) {
		this.consumerInfo = consumerInfo;
		this.ackDao = ackDao;
		this.messageDao = messageDao;
		this.swallowBuffer = swallowBuffer;
		this.threadFactory = threadFactory;
		consumerid = consumerInfo.getConsumerId().getDest().getName();
		topicName = consumerInfo.getConsumerId().getConsumerId();
	}

	@Override
	public void handleAck(final Channel channel, final Long ackedMsgId, final boolean needClose) {

		ackWorker.add(new Runnable() {
			@Override
			public void run() {								    	
				updateMaxMessageId(ackedMsgId);
				if(needClose){
					handleChannelDisconnect(channel);
				}else{
					freeChannels.add(channel);
				}
				
			}
		});	
		
	}
	private void updateMaxMessageId(Long ackedMsgId){	
		if(ackedMsgId != null && ConsumerType.UPDATE_AFTER_ACK.equals(consumerInfo.getConsumerType())){
			ackDao.add(topicName, consumerid, ackedMsgId);
		}
    }
	@Override
	public void handleChannelDisconnect(Channel channel) {
		synchronized(connectedChannels){
			connectedChannels.remove(channel);
		}		
	}
	private void newHandleACKThread(){
		
		HandleACKThread handleACKThread = new HandleACKThread();
    	handleACKThread.setConsumerInformation(this);
    	Thread thread2 = threadFactory.newThread(handleACKThread, consumerInfo.toString() + "-handleACKThread-");
    	thread2.start();
    }
	private void newGetMessageThread(){
		
		GetMessageThread getMessageThread = new GetMessageThread();
		getMessageThread.setConsumerInformation(this);
		Thread thread1 = threadFactory.newThread(getMessageThread, consumerInfo.toString() + "-getMessageThread-");
		thread1.start();
		
	}
	@Override
	public void handleGreet(final Channel channel) {
    	ackWorker.add(new Runnable() {
			@Override
			public void run() {
				synchronized(connectedChannels){
					connectedChannels.add(channel); 
				}		
				freeChannels.add(channel);
				if(!getMessageThreadExist){
					newGetMessageThread();
					getMessageThreadExist = Boolean.TRUE;
				}				
			}
		});
    	if(!handleACKThreadExist){
    		newHandleACKThread();
    		handleACKThreadExist = Boolean.TRUE;
    	}
		
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub
		
	}

	public ArrayBlockingQueue<Runnable> getAckWorker() {
		return ackWorker;
	}

	public void setAckWorker(ArrayBlockingQueue<Runnable> ackWorker) {
		this.ackWorker = ackWorker;
	}

	private long getMessageIdOfTailMessage(String topicName, String consumerId) {
		
		Long maxMessageId = ackDao.getMaxMessageId(topicName, consumerId);
		if(maxMessageId == null){
			maxMessageId = messageDao.getMaxMessageId(topicName);
		}
		if(maxMessageId == null){
			int time = (int)(System.currentTimeMillis() / 1000);
			BSONTimestamp bst = new BSONTimestamp(time, 1);
			maxMessageId = MongoUtils.BSONTimestampToLong(bst);
		}
		return maxMessageId;
	}
	
	@Override
	public void sendMessageByPollFreeChannelQueue(){			
		if(messageQueue == null){			
			long messageIdOfTailMessage = getMessageIdOfTailMessage(topicName, consumerid);
			messageQueue = swallowBuffer.createMessageQueue(topicName, consumerid, messageIdOfTailMessage);
		}
		//线程刚起，第一次调用的时候，需要先去mongo中获取maxMessageId
		try {
			while(true){
				Channel channel = null;
				synchronized(freeChannels){
					if(freeChannels == null){
						break;
					}
					channel = freeChannels.poll(1000,TimeUnit.MILLISECONDS);// TODO
				}			
				if(channel == null){
					break;
					//TODO 用异常替代isConnected
				}else if(channel.isConnected()){
					if(preparedMessage == null){							
						while(true){
							//获得
							message = (SwallowMessage)messageQueue.poll(getMessageInterval, TimeUnit.MILLISECONDS);
							if(message == null){
								getMessageInterval*=2;
							}else {
								getMessageInterval = 1000;
								break;
							}
						}				
						preparedMessage = new PktMessage(consumerInfo.getConsumerId().getDest(), message);
					}
					 Long messageId = message.getMessageId();
					//如果consumer是收到ACK之前更新messageId的类型
					 if(ConsumerType.UPDATE_BEFORE_ACK.equals(consumerInfo.getConsumerType())){
						 ackDao.add(topicName, consumerid, messageId);
					 }						 
					 while(true){
						 if(!channel.isWritable()){
							 if(channel.isConnected()){
								 Thread.sleep(1000);
								 continue;
							 } else {
								 break;
							 }								
						 } else{
								//TODO +isWritable?，连接断开后write后是否会抛异常，isWritable()=false的时候retry, when will write() throw exception?														
							 channel.write(preparedMessage);
							 preparedMessage = null;
							}
					 }
					
				}
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}

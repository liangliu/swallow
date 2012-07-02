package com.dianping.swallow.consumer;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

import com.dianping.swallow.common.codec.JsonDecoder;
import com.dianping.swallow.common.codec.JsonEncoder;
import com.dianping.swallow.common.consumer.ConsumerType;
import com.dianping.swallow.common.message.Destination;
import com.dianping.swallow.common.packet.PktConsumerMessage;
import com.dianping.swallow.common.packet.PktMessage;
import com.dianping.swallow.consumer.netty.MessageClientHandler;

public class ConsumerClient {

	private String consumerId;
	
	private Destination dest;
	
	private ClientBootstrap bootstrap;
	
	private MessageListener listener;
	
	private ConsumerType consumerType;
	
	private InetSocketAddress masterAddress;
	
	private InetSocketAddress slaveAddress;
	
	private Boolean needClose = Boolean.FALSE;
	//consumerClient默认是1个线程处理，如需线程池处理，则另外设置线程数目。
	private int threadCount = 1;
			
	public Boolean getNeedClose() {
		return needClose;
	}

	public void setNeedClose(Boolean needClose) {
		this.needClose = needClose;
	}

	public ConsumerType getConsumerType() {
		return consumerType;
	}

	public void setConsumerType(ConsumerType consumerType) {
		this.consumerType = consumerType;
	}

	public ClientBootstrap getBootstrap() {
		return bootstrap;
	}

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public Destination getDest() {
		return dest;
	}

	public void setDest(Destination dest) {
		this.dest = dest;
	}

	public MessageListener getListener() {
		return listener;
	}

	public void setListener(MessageListener listener) {
		this.listener = listener;
	}

	public int getThreadCount() {
      return threadCount;
   }

   public void setThreadCount(int threadCount) {
      this.threadCount = threadCount;
   }

   public ConsumerClient(String cid, Destination dest, String swallowCAddress){
		this.consumerId = cid;
		this.dest = dest;
		string2Address(swallowCAddress);
	}
	
	/**
	 * 开始连接服务器，同时把连slave的线程启起来。
	 */
	public void beginConnect(){
		init();
	   	ConSlaveThread slave = new ConSlaveThread();
    	slave.setBootstrap(bootstrap);
    	slave.setSlaveAddress(slaveAddress);
	   	Thread slaveThread = new Thread(slave);
	   	slaveThread.start();
	   	while(true){
	   		synchronized(bootstrap){
		   		ChannelFuture future = bootstrap.connect(masterAddress);
		   		future.getChannel().getCloseFuture().awaitUninterruptibly();//等待channel关闭，否则一直阻塞!	
		   	}
	   		try {
				Thread.sleep(1000);//TODO 配置变量
			} catch (InterruptedException e) {
				// TODO 使用LOG
				e.printStackTrace();
			}
	   	}	
	}
	
	//连接swollowC，获得bootstrap
    public void init(){
    	bootstrap = new ClientBootstrap(
                new NioClientSocketChannelFactory(
                        Executors.newCachedThreadPool(),
                        Executors.newCachedThreadPool()));
    	final ConsumerClient cc = this;
        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {  
            @Override  
            public ChannelPipeline getPipeline() throws Exception {  
            	MessageClientHandler handler = new MessageClientHandler(cc);
            	ChannelPipeline pipeline = Channels.pipeline();
	            pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
	            pipeline.addLast("jsonDecoder", new JsonDecoder(PktMessage.class));
	            pipeline.addLast("frameEncoder", new LengthFieldPrepender(4));
	            pipeline.addLast("jsonEncoder", new JsonEncoder(PktConsumerMessage.class));
	            pipeline.addLast("handler", handler);
	            return pipeline;  
            }  
        }); 
    }
    
    private void string2Address(String swallowCAddress){
    	String[] ipAndPorts = swallowCAddress.split(",");
    	String masterIp = ipAndPorts[0].split(":")[0];
    	int masterPort = Integer.parseInt(ipAndPorts[0].split(":")[1]);
    	String slaveIp = ipAndPorts[1].split(":")[0];
    	int slavePort = Integer.parseInt(ipAndPorts[1].split(":")[1]);
    	masterAddress = new InetSocketAddress(masterIp, masterPort);
    	slaveAddress = new InetSocketAddress(slaveIp, slavePort);
    	
    }
}
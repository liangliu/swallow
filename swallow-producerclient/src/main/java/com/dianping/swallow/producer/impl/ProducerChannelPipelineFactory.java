/**
 * Project: swallow-producerclient
 * 
 * File Created at 2012-6-5
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

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.serialization.ObjectDecoder;
import org.jboss.netty.handler.codec.serialization.ObjectEncoder;


/**
 * TODO Comment of ProducerChannelPipelineFactory
 * @author tong.song
 *
 */
public class ProducerChannelPipelineFactory implements ChannelPipelineFactory {
	private Producer producer;
	public ProducerChannelPipelineFactory(Producer producer){
		this.producer = producer;
	}
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		// TODO Auto-generated method stub
		return Channels.pipeline(
				new ObjectEncoder(),
				new ObjectDecoder(), 
				new ProducerHandler(producer));
	}
}

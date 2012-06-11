/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-7-29
 * $Id$
 * 
 * Copyright 2011 dianping.com.
 * All rights reserved.
 *
 * This software is the confidential and proprietary information of
 * Dianping Company. ("Confidential Information").  You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with dianping.com.
 */
package com.dianping.swallow.producer;

import java.io.Serializable;

/***
 * 消息地址
 * @author qing.gu
 *
 */
public class Destination implements Serializable{

	private String name;
	private Type type;

	private enum Type {
		QUEUE, TOPIC
	};

	private Destination() {
	}

	private Destination(String name, Type type) {
		this.name = name;
		this.type = type;
	}

	/***
	 * 创建Queue类型地址
	 * @param name Queue名称
	 * @return
	 */
	public static Destination queue(String name) {
		return new Destination(name, Type.QUEUE);
	}

	/***
	 * 创建Topic类型地址
	 * @param name Topic名称
	 * @return
	 */
	public static Destination topic(String name) {
		return new Destination(name, Type.TOPIC);
	}

	public String getName() {
		return name;
	}

	public boolean isQueue() {
		return type == Type.QUEUE;
	}

	public boolean isTopic() {
		return type == Type.TOPIC;
	}
}

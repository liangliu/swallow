/**
 * Project: ${swallow-client.aid}
 * 
 * File Created at 2011-9-5
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
package com.dianping.swallow.common.dao.impl.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import com.mongodb.ServerAddress;

public class MongoUtil {

	private static Logger log = Logger.getLogger(MongoUtil.class);

	public static List<ServerAddress> parseUri(String uri) {
		uri = uri.trim();
		String schema = "mongodb://";
		if (uri.startsWith(schema)) { // 兼容老各式uri
			uri = uri.substring(schema.length());
		}
		String[] hostPortArr = uri.split(",");
		List<ServerAddress> result = new ArrayList<ServerAddress>();
		for (int i = 0; i < hostPortArr.length; i++) {
			String[] pair = hostPortArr[i].split(":");
			try {
				result.add(new ServerAddress(pair[0].trim(), Integer.parseInt(pair[1].trim())));
			} catch (Exception e) {
				log.error("Bad format of mongo uri", e);
				throw new RuntimeException(e);
			}
		}
		return result;
	}

}

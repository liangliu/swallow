package com.dianping.swallow.consumerserver.util;


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

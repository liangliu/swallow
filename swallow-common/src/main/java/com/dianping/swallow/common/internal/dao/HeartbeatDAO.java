package com.dianping.swallow.common.internal.dao;

import java.util.Date;




public interface HeartbeatDAO {
	
   public Date updateLastHeartbeat(String ip);
	
	public Date findLastHeartbeat(String ip);
}

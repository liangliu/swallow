package com.dianping.swallow.common.dao.impl;

import java.util.Date;

import org.springframework.stereotype.Service;

import com.dianping.swallow.common.dao.HeartbeatDAO;

@Service
public class HeartbeatDAOImpl implements HeartbeatDAO {

	@Override
	public void updateLastHeartbeat(String ip) {
		// TODO Auto-generated method stub
		System.out.println("插到数据库中啦，哈哈哈哈！");
	}

	@Override
	public Date findLastHeartbeat(String ip) {
		// TODO Auto-generated method stub
		return null;
	}

}

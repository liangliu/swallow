package com.dianping.swallow.common.dao.impl;

import org.bson.types.BSONTimestamp;
import org.springframework.stereotype.Service;

import com.dianping.swallow.common.dao.CounterDAO;
/**
 * @author zhang.yu
 * 单例实现，因为dao不多，就不用工厂类或者Spring管理了吧！
 */
@Service
public class CounterDAOImpl implements CounterDAO {

	private static CounterDAO counterDAO = new CounterDAOImpl();
	
	private CounterDAOImpl(){		
	}
	
	public static CounterDAO getCounterDAO(){
		return counterDAO;
	}
	@Override
	public BSONTimestamp getMaxTimeStamp(String topicId, String consumerId) {
		// TODO Auto-generated method stub
		BSONTimestamp timeStamp = new BSONTimestamp();
		return timeStamp;
	}

	@Override
	public int addCounter(String topicId, String consumerId, BSONTimestamp maxTStamp) {
		// TODO Auto-generated method stub
		return 0;
	}



}

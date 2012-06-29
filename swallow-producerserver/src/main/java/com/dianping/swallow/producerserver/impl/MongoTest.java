/**
 * Project: swallow-client
 * 
 * File Created at 2012-5-31
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
package com.dianping.swallow.producerserver.impl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.bson.types.BSONTimestamp;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.Mongo;
import com.mongodb.ServerAddress;

/**
 * TODO Comment of MongoTest
 * @author tong.song
 *
 */
public class MongoTest {
	private int finished = 0;
	private int threadseq = 0;
	List addr = new ArrayList();
	public MongoTest(){
		try {
			addr.add(new ServerAddress("192.168.31.178", 27016));
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}		
	}
	public void mongoTest(int threadNum, int rows){
//		Mongo test = null;
//		try{
//			test = new Mongo(addr);
//		}catch(Exception e){
//			System.out.println("can't create mongo object.");
//		}
//		DBCollection coll = test.getDB("songtong").getCollection("test");
//		coll.drop();
		
		ExecutorService threadPool = Executors.newFixedThreadPool(threadNum);
		for(int i = 0; i < threadNum; i++){
			threadPool.execute(new task(rows));
		}
		threadPool.shutdown();

//		while(finished != threadNum){}
//		System.out.println("All threads done.");
//		DBCursor cur = coll.find();
//		cur.batchSize(1000);
//		int wrongtimes = 0;
//		BSONTimestamp bst = new BSONTimestamp();
//		BSONTimestamp bstb = new BSONTimestamp();
//		BasicDBObject dbo = null;
//		while(cur.hasNext()){
//			dbo = (BasicDBObject)cur.next();
//			bst = (BSONTimestamp)dbo.get("Timestamp");
//			if(bst.getTime() < bstb.getTime() || ( bst.getTime() == bstb.getTime() && bst.getInc() <= bstb.getInc() )){
//				System.out.println("No inc for " + (++wrongtimes));
//				System.out.println("\tCur: " + bst.getTime() + ", " + bst.getInc());
//				System.out.println("\tBefore: " + bstb.getTime() + ", " + bstb.getInc());
//			}
//			bstb = bst;
//		}
//		if(wrongtimes == 0){
//			System.out.println("All right.");
//		}else{
//			System.out.println("Exist problems.");
//		}
	}
	
	private class task implements Runnable {
		private int rows;
		private int seq = ++threadseq;
		
		public task(int rows){
			this.rows = rows;
		}
		public void run() {
			Mongo test = null;
			try{
				test = new Mongo(addr);
			}catch(Exception e){
				System.out.println("can't create mongo object.");
			}
			
			DBCollection coll = test.getDB("songtong").getCollection("test");
			
			for(int i = 0; i < rows/2; i++){
				BasicDBObject bDbo = new BasicDBObject();
				bDbo.put("Timestamp", new BSONTimestamp());	
				bDbo.put("SEQ", seq);
				coll.insert(bDbo);
				
				BasicDBObject bDbo2 = new BasicDBObject();
				bDbo2.put("Timestamp", new BSONTimestamp());
				bDbo2.put("SEQ", (seq + 0.5));
				coll.insert(bDbo2);
			}
			
//			try{
//				Thread.sleep(1000);
//			}catch(Exception e){}
			
//			BasicDBObject bo = new BasicDBObject().append("SEQ", (seq + 0.5));
//			BasicDBObject bo2 = new BasicDBObject().append("$set", new BasicDBObject().append("SEQ", seq));
//			WriteResult update = coll.update(bo, bo2, false, true);
//			System.out.println(coll.count(bo));
//			System.out.println(update);
			
//			coll.remove(bo);
			
//			for(int i = 0; i < rows/2; i++){
//				BasicDBObject bDbo = new BasicDBObject();
//				bDbo.put("Timestamp", new BSONTimestamp());
//				bDbo.put("SEQ", seq);
//				coll.insert(bDbo);
//			}
			finished++;
			System.out.println(seq + " Done.");
		}
	}
	
	public static void main(String[] args){
		MongoTest mt = new MongoTest();
		mt.mongoTest(100, 1000);
	}
}

package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.ObjectId;

import com.dianping.swallow.common.message.JsonBinder;

public class TestObjectId {

   /**
    * @param args
    */
   public static void main(String[] args) {
ObjectId oid = new ObjectId();
      
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      String json = jsonBinder.toJson(oid);
      System.out.println(json);
      
      ObjectId oid2 =jsonBinder.fromJson(json, ObjectId.class);
   }

}

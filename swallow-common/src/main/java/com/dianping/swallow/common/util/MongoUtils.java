package com.dianping.swallow.common.util;

import org.bson.types.BSONTimestamp;

public class MongoUtils {

   public static BSONTimestamp longToBSONTimestamp(Long messageId) {
      int time = (int) (messageId >>> 32);
      int inc = (int) (messageId & 0xFFFFFFFF);
      BSONTimestamp timestamp = new BSONTimestamp(time, inc);
      return timestamp;
   }

   public static Long BSONTimestampToLong(BSONTimestamp timestamp) {
      int time = timestamp.getTime();
      int inc = timestamp.getInc();
      Long messageId = ((long) time << 32) | inc;
      return messageId;
   }

   public static void main(String[] args) {
      System.out.println(MongoUtils.longToBSONTimestamp(5759378400351354888L));
   }
}

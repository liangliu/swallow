package com.dianping.swallow.common.internal.util;

import org.bson.types.BSONTimestamp;

public class MongoUtils {
   private MongoUtils() {
   }

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

   public static Long getLongByCurTime() {
      int time = (int) (System.currentTimeMillis() / 1000);
      BSONTimestamp bst = new BSONTimestamp(time, 1);
      return BSONTimestampToLong(bst);
   }

   public static void main(String[] args) {
      System.out.println(MongoUtils.longToBSONTimestamp(5759378400351354888L));
   }
}

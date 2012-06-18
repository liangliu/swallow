package com.dianping.swallow.common.dao.impl.mongodb;

import org.bson.types.BSONTimestamp;

public class MessageId {
   private int inc;
   private int time;

   public static MessageId fromBSONTimestamp(BSONTimestamp timestamp) {
      MessageId messageId = new MessageId();
      messageId.setInc(timestamp.getInc());
      messageId.setTime(timestamp.getTime());
      return messageId;
   }

   public static BSONTimestamp toBSONTimestamp(MessageId messageId) {
      BSONTimestamp timestamp = new BSONTimestamp(messageId.getTime(), messageId.getInc());
      return timestamp;
   }

   public int getInc() {
      return inc;
   }

   public int getTime() {
      return time;
   }

   public void setInc(int inc) {
      this.inc = inc;
   }

   public void setTime(int time) {
      this.time = time;
   }

   @Override
   public String toString() {
      return "MessageId [inc=" + inc + ", time=" + time + "]";
   }

}

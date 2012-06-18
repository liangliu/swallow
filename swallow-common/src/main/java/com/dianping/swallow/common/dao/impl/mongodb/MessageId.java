package com.dianping.swallow.common.dao.impl.mongodb;

import java.nio.ByteBuffer;

import org.bson.types.ObjectId;

public class MessageId {

   private int timeSecond;
   private int machine;
   private int inc;

   public int getTimeSecond() {
      return timeSecond;
   }

   public void setTimeSecond(int timeSecond) {
      this.timeSecond = timeSecond;
   }

   public int getMachine() {
      return machine;
   }

   public void setMachine(int machine) {
      this.machine = machine;
   }

   public int getInc() {
      return inc;
   }

   public void setInc(int inc) {
      this.inc = inc;
   }

   public static MessageId fromObjectId(ObjectId objectId) {
      MessageId messageId = new MessageId();
      messageId.setInc(objectId.getInc());
      messageId.setTimeSecond(objectId.getTimeSecond());
      messageId.setMachine(objectId.getMachine());
      return messageId;
   }

   public static ObjectId toObjectId(MessageId messageId) {
      ObjectId objectId = new ObjectId(messageId.getTimeSecond(), messageId.getMachine(), messageId.getInc());
      return objectId;
   }

   public byte[] toByteArray() {
      byte b[] = new byte[12];
      ByteBuffer bb = ByteBuffer.wrap(b);
      // by default BB is big endian like we need
      bb.putInt(timeSecond);
      bb.putInt(machine);
      bb.putInt(inc);
      return b;
   }

   public String toStringMongod() {
      byte b[] = toByteArray();

      StringBuilder buf = new StringBuilder(24);

      for (int i = 0; i < b.length; i++) {
         int x = b[i] & 0xFF;
         String s = Integer.toHexString(x);
         if (s.length() == 1)
            buf.append("0");
         buf.append(s);
      }

      return buf.toString();
   }

   public String toString() {
      return toStringMongod();
   }

}

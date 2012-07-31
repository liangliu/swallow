package com.dianping.swallow.common.internal.packet;

import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.message.Destination;

public class PktMessage extends Packet implements Message {

   private static final long serialVersionUID = 2189810352053398027L;
   private SwallowMessage    content;
   private Destination       destination;
   private String            catEventID;

   public PktMessage() {
      super.setPacketType(PacketType.OBJECT_MSG);
   }

   public PktMessage(Destination dest, SwallowMessage content) {
      super.setPacketType(PacketType.OBJECT_MSG);

      this.setDestination(dest);
      this.setContent(content);
   }

   @Override
   public SwallowMessage getContent() {
      return content;
   }

   @Override
   public Destination getDestination() {
      return destination;
   }

   @Override
   public String toString() {
      return String.format("PktMessage [content=%s, dest=%s]", getContent(), getDestination());
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((getContent() == null) ? 0 : getContent().hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj) {
         return true;
      }
      if (obj == null) {
         return false;
      }
      if (!(obj instanceof PktMessage)) {
         return false;
      }
      PktMessage other = (PktMessage) obj;
      if (getContent() == null) {
         if (other.getContent() != null) {
            return false;
         }
      } else if (!getContent().equals(other.getContent())) {
         return false;
      }
      return true;
   }

   public String getCatEventID() {
      return catEventID;
   }

   public void setCatEventID(String catEventID) {
      this.catEventID = catEventID;
   }

   public void setDestination(Destination destination) {
      this.destination = destination;
   }

   public void setContent(SwallowMessage content) {
      this.content = content;
   }
}

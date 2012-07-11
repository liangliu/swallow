package com.dianping.swallow.common.internal.packet;

import com.dianping.swallow.common.internal.message.SwallowMessage;
import com.dianping.swallow.common.message.Destination;

public class PktMessage extends Packet implements Message {

   private static final long serialVersionUID = 2189810352053398027L;
   private SwallowMessage    content;
   private Destination       dest;

   public PktMessage() {
      super();
   }

   public PktMessage(Destination dest, SwallowMessage content) {
      super.setPacketType(PacketType.OBJECT_MSG);

      this.dest = dest;
      this.content = content;
   }

   @Override
   public SwallowMessage getContent() {
      return content;
   }

   @Override
   public Destination getDestination() {
      return dest;
   }

   @Override
   public String toString() {
      return String.format("PktMessage [content=%s, dest=%s]", content, dest);
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((content == null) ? 0 : content.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (!(obj instanceof PktMessage))
         return false;
      PktMessage other = (PktMessage) obj;
      if (content == null) {
         if (other.content != null)
            return false;
      } else if (!content.equals(other.content))
         return false;
      return true;
   }

}

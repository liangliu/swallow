package com.dianping.swallow.common.internal.packet;

import com.dianping.swallow.common.message.Destination;

public interface Message {
   Object getContent();

   Destination getDestination();
}

package com.dianping.swallow.common.message;

import org.json.simple.JSONObject;

public class JsonMessage extends AbstractMessage<JSONObject> {

   private static final long serialVersionUID = -4756799841621304498L;

   private JSONObject        content;

   public JSONObject getContent() {
      return content;
   }

   public void setContent(JSONObject content) {
      this.content = content;
   }

}

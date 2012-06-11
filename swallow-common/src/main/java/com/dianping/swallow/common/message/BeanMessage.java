package com.dianping.swallow.common.message;

public class BeanMessage extends TextMessage {

   private static final long serialVersionUID = 2465173281205737612L;

   public BeanMessage() {
      this.setContentType(Message.ContentType.BeanMessage);
   }

   /**
    * 将一个普通的bean序列化成json字符串，作为content存放起来。
    * <p>
    * <b>注意:</b>
    * bean可以是java常见类型如String,Double,Integer,Byte,Boolean,集合类型Map,Set,List；
    * 也是可以是自定义的bean，但需要符合POJO的定义。
    * </p>
    * 
    * @param bean 序列化后作为content存放的java对象
    */
   public void writeBeanAsJsonString(Object bean) {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      this.setContent(jsonBinder.toJson(bean));
   }

   /**
    * 将content内容作为json字符串反序列化成指定Class类型的bean
    * 
    * @param clazz 指定反序列化后的Class类型
    * @return 返回将content内容作为json字符串反序列化后的bean
    */
   public <T> T readBean(Class<T> clazz) {
      JsonBinder jsonBinder = JsonBinder.buildNormalBinder();
      return jsonBinder.fromJson(this.getContent(), clazz);
   }

}

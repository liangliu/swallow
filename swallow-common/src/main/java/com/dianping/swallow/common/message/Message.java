package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Map;

public interface Message {

   /**
    * 获取消息id
    * 
    * @return
    */
   Long getMessageId();

   /**
    * 获取消息创建时间
    * 
    * @return
    */
   Date getGeneratedTime();

   /**
    * 获取消息<em>发送方</em>使用的Swallow的版本号
    * 
    * @return
    */
   String getVersion();

   /**
    * 获取消息<em>发送方</em>添加的消息属性(以key-value形式存储)
    * 
    * @return
    */
   Map<String, String> getProperties();

   /**
    * 以字符串的格式获取消息内容(如果<em>发送方</em>发送的消息内容非String类型，将被转化成json字符串)
    * 
    * @return
    */
   String getContent();

   /**
    * 如果<em>发送方</em>发送的消息内容非String类型，Swallow内部将会把它转化成<em>json字符串</em>(通过
    * <code>getContent()</code> 可以获得到)进行传输， 需要获取原始对象，可以调用此方法将<em>json字符串</em>
    * 反序列化成原始对象，不过，这要求您知道<em>发送方</em>发送的消息内容的Class类型。
    * 
    * @param clazz <em>发送方</em>发送的消息内容的Class类型
    * @return <em>发送方</em>发送的原始对象
    */
   <T> T transferContentToBean(Class<T> clazz);

   /**
    * 获取<em>消息内容</em>的经过<em>Sha1</em>算法生成的消息摘要。此方法为辅助方法，在适当场景可以用于检验<em>消息内容</em>
    * 是否重复。
    * 
    * @return
    */
   String getSha1();

   /**
    * 获取消息<em>发送方</em>的IP地址
    * 
    * @return
    */
   String getSourceIp();
}

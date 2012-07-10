package com.dianping.swallow.common.message;

import java.util.Date;
import java.util.Map;

/**
 * Swallow消息接口。(由Swallow内部实现该接口的具体类，API用户请勿实现该接口。)
 * 
 * @author wukezhu
 */
public interface Message {

   /**
    * 获取消息id
    * 
    * @return 消息id
    */
   Long getMessageId();

   /**
    * 获取消息创建时间
    * 
    * @return 消息创建时间
    */
   Date getGeneratedTime();

   /**
    * 获取消息类型
    * 
    * @return 消息类型
    */
   String getType();

   /**
    * 获取消息<em>发送方</em>添加的消息属性(以key-value形式存储)
    * 
    * @return 消息<em>发送方</em>添加的消息属性
    */
   Map<String, String> getProperties();

   /**
    * 返回字符串格式的消息内容(如果<em>发送方</em>发送的消息内容非String类型，将被转化成json字符串)
    * 
    * @return 消息内容
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
    * @return 经过<em>Sha1</em>算法生成的消息摘要
    */
   String getSha1();

   /**
    * 获取消息<em>发送方</em>的IP地址
    * 
    * @return 消息<em>发送方</em>的IP地址
    */
   String getSourceIp();
}

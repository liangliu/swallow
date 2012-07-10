package com.dianping.swallow.common.producer.exceptions;

/**
 * Producer工作在同步模式时，调用SendMessage函数，如果swallow不能将message保存至数据库，则抛出该异常
 * <br>可能的原因包括：数据库超过最大负荷或swallow与数据库的网络连接出现问题</br>
 * @author Icloud
 *
 */
public class ServerDaoException extends RuntimeException {

   private static final long serialVersionUID = -35938089521851134L;

   @Override
   public String getMessage() {
      return "Can not save message to DB now.";
   }

}

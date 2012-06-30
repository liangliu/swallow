package com.dianping.swallow;

/**
 * 当consumer无法处理Message且希望MQ暂存该消息时抛出该异常
 * 
 * @author qing.gu
 * 
 */
public class BackoutMessageException extends Exception {

   private static final long serialVersionUID = -1029955042149831812L;

   public enum ScheduleType {
		/**
		 * 在指定的秒数后推送
		 */
		SecondsAfter,
		/**
		 * 需要通过控制台操作后才进行推送
		 */
		Manual
	};

	private ScheduleType schedule;
	private int seconds;

	/**
	 * 
	 * @param schedule 手动或者自动推送该消息
	 * @param seconds 指定自动推送的延迟时间，单位：秒
	 * @param message 出错的原因，供排查，建议使用较少的出错原因，以方便更具出错原因进行聚合操作
	 * @param cause 出错现场的异常
	 */
	public BackoutMessageException(ScheduleType schedule, int seconds, String message, Throwable cause) {
		super(message, cause);
		this.schedule = schedule;
		this.seconds = seconds;
	}

	/**
	 * 
	 * @param schedule 手动或者自动推送该消息
	 * @param seconds 指定自动推送的延迟时间，单位：秒
	 * @param message 出错的原因，供排查，建议使用较少的出错原因，以方便更具出错原因进行聚合操作
	 */
	public BackoutMessageException(ScheduleType schedule, int seconds, String message) {
		super(message);
		this.schedule = schedule;
		this.seconds = seconds;
	}

	public ScheduleType getSchedule() {
		return schedule;
	}

	public int getSeconds() {
		return seconds;
	}

}

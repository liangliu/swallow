package com.dianping.swallow.consumerserver;



import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import com.dianping.hawk.jmx.HawkJMXUtil;
//import com.dianping.swallow.monitor.thread.SwallowThreadStatusBean;

/**
 * 管理所有swallow线程，提供jmx监控
 * @author qing.gu
 *
 */
public class MQThreadFactory implements ThreadFactory {

	private static Logger log = Logger.getLogger(MQThreadFactory.class);

	private static ThreadGroup topThreadGroup = new ThreadGroup("swallow-top");
	private final static String PREFIX = "swallow-thread-";

	private List<WeakReference<Thread>> threadList = Collections
			.synchronizedList(new ArrayList<WeakReference<Thread>>());
	private ConcurrentHashMap<String, AtomicInteger> prefixToSeq = new ConcurrentHashMap<String, AtomicInteger>();

	public MQThreadFactory() {
		//暂时注掉监控那玩意。
//		try {
//			HawkJMXUtil.registerMBean(new SwallowThreadStatusBean());
//		} catch (Exception e) {
//			log.error("error register jxm bean", e);
//		}
	}

	public static ThreadGroup getTopThreadGroup() {
		return topThreadGroup;
	}

	@Override
	public Thread newThread(Runnable r) {
		return newThread(r, PREFIX);
	}

	public Thread newThread(Runnable r, String threadNamePrefix) {
		prefixToSeq.putIfAbsent(threadNamePrefix, new AtomicInteger(1));
		Thread t = new Thread(topThreadGroup, r, PREFIX + threadNamePrefix
				+ prefixToSeq.get(threadNamePrefix).getAndIncrement());
		threadList.add(new WeakReference<Thread>(t));
		return t;
	}

	public void close() {
		for (WeakReference<Thread> ref : threadList) {
			Thread t = ref.get();
			if (t != null && t.isAlive()) {
				t.interrupt();
			}
		}
	}

}

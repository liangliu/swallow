package com.dianping.swallow.producer;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import com.dianping.swallow.common.util.Destination;
import com.dianping.swallow.producer.impl.Producer;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testApp()
    {
        assertTrue( true );
    }
    
    private class task implements Runnable{
    	String content;
    	public task(String content){
    		this.content = content;
    	}
		@Override
		public void run() {
			// TODO Auto-generated method stub
		Producer ps = Producer.getInstance();
		while(true){
//			content += i++;
			System.out.println(ps.sendMessage(Destination.queue("master.slave"), content));
			try {
				Thread.sleep(1000);
			} catch (Exception e) {
				// TODO: handle exception
			}
		}
		}
    }
    
    public void doTest(){
    	for(int i = 0; i < 2; i++){
    		String newContent = "NO: " + i;
    		Thread td = new Thread(new task(newContent));
    		td.start();
    	}
    }
    
    public static void main(String[] args) {
    	new AppTest("111").doTest();
	}
}

package com.dianping.swallow.common.internal.monitor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 监听端口，当接收到匹配的命令时触发给定的CloseHook
 * 
 * @author qing.gu
 */
public class CloseMonitor {

   private static final Logger LOG                  = LoggerFactory.getLogger(CloseMonitor.class);

   private final static String DEFAULT_SHUTDOWN_CMD = "shutdown";

   public interface CloseHook {
      void onClose();
   }

   public void start(int port, CloseHook hook) {
      start(port, DEFAULT_SHUTDOWN_CMD, hook);
   }

   public void start(final int port, final String cmd, final CloseHook hook) {
      Thread t = new Thread() {
         @Override
         public void run() {
            try {
               ServerSocket ss = new ServerSocket(port);

               LOG.info("MonitorTask started at port: " + port);

               Socket socket = null;

               while (true) {
                  try {
                     socket = ss.accept();
                     LOG.info("Accepted one connection : " + socket.getRemoteSocketAddress());
                     BufferedReader br = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                     String command = br.readLine();
                     LOG.info("Command : " + command);
                     if (cmd.equals(command)) {
                        LOG.info("Shutdown command received.");
                        break;
                     }
                  } catch (Exception e) {
                     // ignore
                  } finally {
                     if (socket != null) {
                        socket.close();
                     }
                  }
               }
               hook.onClose();
               System.exit(0);
            } catch (Exception e) {
               LOG.error("CloseMonitor start failed.", e);
            }
         }
      };
      t.setDaemon(true);
      t.setName("CloseMonitor");
      t.start();

   }

}

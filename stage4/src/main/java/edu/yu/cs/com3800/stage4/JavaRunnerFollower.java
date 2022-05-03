package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread {
    ZooKeeperPeerServerImpl server;
    JavaRunner javaRunner;
    boolean shutdown = false;
    Logger logger;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, Logger logger) throws IOException {
        this.server = server;
        this.javaRunner = new JavaRunner();
        this.logger = logger;
    }
    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, Path targetDir, Logger logger) throws IOException {
        this.server = server;
        this.javaRunner = new JavaRunner(targetDir);
        this.logger = logger;
    }

    @Override
    public void run() {
        // Create a server socket to accept client connection requests
        ServerSocket servSock = null;
        try {
            servSock = new ServerSocket(server.myPort);
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert servSock != null;
        int BUFSIZE = 2048;
        byte[] receiveBuf = new byte[BUFSIZE];  // Receive buffer

        while(!this.isInterrupted() && server.getPeerState() == ZooKeeperPeerServer.ServerState.FOLLOWING && !shutdown) {
            try {
                Socket clntSock = servSock.accept();
                InputStream in = clntSock.getInputStream();
                OutputStream out = clntSock.getOutputStream();
                Message m = new Message(Util.readAllBytesFromNetwork(in));
                logger.info("Received request for message ID " + m.getRequestID());
                String response;
                boolean errorOccurred = false;
                try {
                    response = javaRunner.compileAndRun(new ByteArrayInputStream(m.getMessageContents()));
                } catch (Exception e) {
                    response = e.getMessage() + "\n" + Util.getStackTrace(e);
                    errorOccurred = true;
                }
                out.write(new Message(Message.MessageType.COMPLETED_WORK, response.getBytes(), m.getReceiverHost(), m.getReceiverPort(), m.getSenderHost(), m.getSenderPort(), m.getRequestID(), errorOccurred).getNetworkPayload());
                logger.info("Returned request for message ID " + m.getRequestID());
            }
            catch (Exception e) {
                logger.severe(Util.getStackTrace(e));
            }
        }
        try {
            servSock.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void shutdown() {
        this.shutdown = true;
        interrupt();
    }
}

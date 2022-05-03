package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread {
    ZooKeeperPeerServerImpl server;
    JavaRunner javaRunner;
    boolean shutdown = false;
    Logger logger;
    Queue<Message> queuedUpWork = new LinkedList<>();
    ServerSocket servSock = null;
    Socket clntSock = null;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, Queue<Message> queuedUpWork, Logger logger) throws IOException {
        this.server = server;
        this.javaRunner = new JavaRunner();
        this.queuedUpWork.addAll(queuedUpWork);
        this.logger = logger;
    }
    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, Path targetDir, Queue<Message> queuedUpWork, Logger logger) throws IOException {
        this.server = server;
        this.javaRunner = new JavaRunner(targetDir);
        this.queuedUpWork.addAll(queuedUpWork);
        this.logger = logger;
    }

    @Override
    public void run() {
        logger.info("Entered JavaRunnerFollower run method");
        // Create a server socket to accept client connection requests
        try {
            servSock = new ServerSocket(server.myPort);
            servSock.setReuseAddress(true);
            logger.info("Created server socket for runner");
        } catch (IOException e) {
            e.printStackTrace();
        }
        assert servSock != null;

        try {
            while(!this.isInterrupted() && !shutdown) {
                logger.info("Looking to accept connections/requests from leader");
                clntSock = servSock.accept();
                InputStream in = clntSock.getInputStream();
                OutputStream out = clntSock.getOutputStream();
                Message m = new Message(Util.readAllBytesFromNetwork(in));
                if(m.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                    logger.info("Received NEW_LEADER_GETTING_LAST_WORK request from new leader");
//                    System.out.println("Received NEW_LEADER_GETTING_LAST_WORK request from new leader");
                    Message polled;
                    while((polled = queuedUpWork.poll()) != null) {
                        out.write(polled.getNetworkPayload());
                        logger.info("Sent completed message with request ID " + polled.getRequestID() + " to new leader");
                    }
                    out.write(new byte[0]);
                    logger.info("Sent empty byte array");
                    out.close();
                    in.close();
                    clntSock.close();
                    continue;
                }
//                System.out.println("Server " + server.getServerId() + " received request for message ID " + m.getRequestID());
                logger.info("Received request for message ID " + m.getRequestID());
                String response;
                boolean errorOccurred = false;
                try {
                    response = javaRunner.compileAndRun(new ByteArrayInputStream(m.getMessageContents()));
                } catch (Exception e) {
                    response = e.getMessage() + "\n" + Util.getStackTrace(e);
                    errorOccurred = true;
                }
                Message resp = new Message(Message.MessageType.COMPLETED_WORK, response.getBytes(), m.getReceiverHost(), m.getReceiverPort(), m.getSenderHost(), m.getSenderPort(), m.getRequestID(), errorOccurred);
                queuedUpWork.add(resp);
                if(this.isInterrupted() || shutdown) {
                    logger.info("Shut down without returning work");
                    clntSock.close();
                    break;
                }
                out.write(new Message(Message.MessageType.COMPLETED_WORK, response.getBytes(), m.getReceiverHost(), m.getReceiverPort(), m.getSenderHost(), m.getSenderPort(), m.getRequestID(), errorOccurred).getNetworkPayload());
                queuedUpWork.remove(resp);
                logger.info("Returned request for message ID " + m.getRequestID());
                out.close();
                in.close();
            }
        } catch (Exception e) {
            logger.severe(Util.getStackTrace(e));
        }
        finally {
            logger.info("Exiting running loop");

        }
    }

//    public void setLeaderDead() {
//        leaderIsDead = true;
//        logger.warning("Leader is dead");
//    }

    public void shutdown() {
        this.shutdown = true;
        interrupt();
        if(clntSock != null && !clntSock.isClosed()) {
            try {
                clntSock.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            if(servSock != null && !servSock.isClosed()) {
                servSock.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
//        System.out.println("Closed server socket on server ID " + server.getServerId());
        logger.info("Closed server socket");
    }
}

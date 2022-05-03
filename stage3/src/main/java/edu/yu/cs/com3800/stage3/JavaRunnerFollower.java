package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.JavaRunner;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;
import edu.yu.cs.com3800.stage3.ZooKeeperPeerServerImpl;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.LinkedBlockingQueue;

public class JavaRunnerFollower extends Thread {
    ZooKeeperPeerServerImpl server;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    JavaRunner javaRunner;
    boolean shutdown = false;

    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages) throws IOException {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.javaRunner = new JavaRunner();
    }
    public JavaRunnerFollower(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, LinkedBlockingQueue<Message> outgoingMessages, Path targetDir) throws IOException {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.javaRunner = new JavaRunner(targetDir);
    }

    @Override
    public void run() {
        while(server.getPeerState() == ZooKeeperPeerServer.ServerState.FOLLOWING && !shutdown) {
            Message m = incomingMessages.poll();
            if(m == null) continue;
            String response;
            try {
                response = javaRunner.compileAndRun(new ByteArrayInputStream(m.getMessageContents()));
            } catch (Exception e) {
                response = e.getMessage() + "\n" + Util.getStackTrace(e);
//                logger.info(ret);
            }
            boolean successful = outgoingMessages.offer(new Message(Message.MessageType.COMPLETED_WORK, response.getBytes(), m.getReceiverHost(), m.getReceiverPort(), m.getSenderHost(), m.getSenderPort(), m.getRequestID()));
            if(!successful) {
                int a = 4;
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}

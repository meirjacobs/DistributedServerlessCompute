package edu.yu.cs.com3800.stage3;

import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class RoundRobinLeader extends Thread {
    ZooKeeperPeerServerImpl server;
    LinkedBlockingQueue<Message> incomingMessages;
    LinkedBlockingQueue<Message> outgoingMessages;
    List<Long> workerServerIDs;
    Map<Long, InetSocketAddress> requestIDToRequesterAddress = new HashMap<>();
    boolean shutdown = false;

    private static volatile long requestID = 0;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages,
                            LinkedBlockingQueue<Message> outgoingMessages, List<Long> workerServerIDs) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.outgoingMessages = outgoingMessages;
        this.workerServerIDs = workerServerIDs;
    }

    @Override
    public void run() {
        Message m;
        int counter = 0;
        while(server.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING && !shutdown) {
            if((m = incomingMessages.poll()) == null) continue;
            // If work message
            if(m.getMessageType() == Message.MessageType.WORK) {
                long reqID;
                synchronized (this) {
                    reqID = requestID++;
                }
                InetSocketAddress target = server.getPeerByID(workerServerIDs.get(counter++));
                Message newWorkMessage = new Message(Message.MessageType.WORK, m.getMessageContents(), m.getSenderHost(), m.getSenderPort(), target.getHostName(), target.getPort(), reqID);
                requestIDToRequesterAddress.put(newWorkMessage.getRequestID(), new InetSocketAddress(newWorkMessage.getSenderHost(), newWorkMessage.getSenderPort()));
                outgoingMessages.offer(newWorkMessage);
                if(counter >= workerServerIDs.size()) counter = 0;
            }
            // If completed work message
            else if(m.getMessageType() == Message.MessageType.COMPLETED_WORK) {
                assert requestIDToRequesterAddress.get(m.getRequestID()) != null;
                server.sendMessage(Message.MessageType.COMPLETED_WORK, m.getMessageContents(), requestIDToRequesterAddress.get(m.getRequestID()));
                requestIDToRequesterAddress.remove(m.getRequestID());
            }
            // If some other message just put back in queue and will be dealt with in a later stage
            else {
                incomingMessages.offer(m);
            }
        }
    }

    public void shutdown() {
        this.shutdown = true;
    }
}

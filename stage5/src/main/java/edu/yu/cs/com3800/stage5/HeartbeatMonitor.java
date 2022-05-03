package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class HeartbeatMonitor extends Thread implements LoggingServer {
    private Map<Long, Long> idToLastHeartbeat = new ConcurrentHashMap<>();
    private Map<Long, Long> idToLastHeartbeatTime = new ConcurrentHashMap<>();
    private ZooKeeperPeerServerImpl server;
    private LinkedBlockingQueue<Message> incomingHeartbeats;
    private Set<Long> failedServersIDs = new HashSet<>();
    private Set<InetSocketAddress> failedServersAddresses = new HashSet<>();
    private List<InetSocketAddress> heartbeatDestination;
    private long heartbeatCounter = 0;
    private int heartbeatDestinationCounter = 0;
    private ReceivingHeartbeats receivingHeartbeats;
    private List<GossipMessageInfo> listOfReceivedGossips = new ArrayList<>();
    private Logger gossipLogger;
    private volatile boolean shutdown = false;
    private Logger logger;
    static final int GOSSIP = 3000;
    static final int FAIL = GOSSIP * 10;
    static final int CLEANUP = FAIL * 2;

    public HeartbeatMonitor(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingHeartbeats) throws IOException {
        this.server = server;
        this.incomingHeartbeats = incomingHeartbeats;
        this.logger = initializeLogging(Util.getLoggerName(HeartbeatMonitor.class.getSimpleName(), server.getServerId(), server.getUdpPort()));
        this.heartbeatDestination = new ArrayList<>();
        this.heartbeatDestination.addAll(this.server.peerIDtoAddress.values());
        Collections.shuffle(this.heartbeatDestination);
        logger.info(this.heartbeatDestination.toString());
        receivingHeartbeats = new ReceivingHeartbeats(incomingHeartbeats);
        File theDir = new File(System.getProperties().getProperty("user.dir") + File.separator + "gossipLogs");
        if (!theDir.exists()){
            theDir.mkdirs();
        }
        this.gossipLogger = initializeLogging(System.getProperties().getProperty("user.dir") + File.separator + "gossipLogs" + File.separator + "server" + server.getServerId() + ".log");
    }

    @Override
    public void run() {
        Util.startAsDaemon(receivingHeartbeats, "ReceivingHeartbeats on server " + server.getServerId());
        long serverID = server.getServerId();
//        Random random = new Random();
        while(!shutdown) {
            idToLastHeartbeat.put(serverID, ++heartbeatCounter);
            InetSocketAddress address = heartbeatDestination.get(incrementHeartbeatDestinationCounter());
            while(address.equals(server.myAddress) || isPeerDead(address)) address = heartbeatDestination.get(incrementHeartbeatDestinationCounter());
            server.sendMessage(Message.MessageType.GOSSIP, createGossipMessageContents(), address);
            logger.info("Sent heartbeat data (" + heartbeatCounter + ") to server " + address.getPort());
            try {
                Thread.sleep(GOSSIP);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        System.out.println("Exited gossiping loop");
    }

    public void shutdown() {
//        System.out.println("shutting down");
        shutdown = true;
        receivingHeartbeats.shutdown();
        interrupt();
    }

    private int incrementHeartbeatDestinationCounter() {
        if(heartbeatDestinationCounter < heartbeatDestination.size() - 1) ++heartbeatDestinationCounter;
        else heartbeatDestinationCounter = 0;
        return heartbeatDestinationCounter;
    }

    public void updatePeerHeartbeatStatuses(Message message) {
        if(message.getMessageType() != Message.MessageType.GOSSIP) return;
        byte[] contents = message.getMessageContents();
        ByteBuffer buffer = ByteBuffer.wrap(contents);
        buffer.clear();
        long senderID = buffer.getLong();
        GossipMessageInfo gossipMessageInfo =new GossipMessageInfo(senderID, contents, System.currentTimeMillis());
        listOfReceivedGossips.add(gossipMessageInfo);
        gossipLogger.info(gossipMessageInfo.toString());
        if(isPeerDead(senderID)) return;
        logger.info("Received heartbeat metrics from server "  + senderID);
        long currentTime = System.currentTimeMillis();
        while(buffer.hasRemaining()) {
            long id = buffer.getLong();
            long lastHeartbeat = buffer.getLong();
            if(id == server.getServerId()) continue;
            if(isPeerDead(id)) continue;
            logger.info("Server " + id + ": Last heartbeat: " + lastHeartbeat + ", This heartbeat counter: " + heartbeatCounter);
            logger.info("Server " + id + ": Last heartbeat time: " +  + idToLastHeartbeatTime.getOrDefault(id, 0L) + ", Current Time: " + currentTime
            + "Diff: " + (currentTime - idToLastHeartbeatTime.getOrDefault(id, 0L)));
            if(!idToLastHeartbeat.containsKey(id) || lastHeartbeat > idToLastHeartbeat.get(id)) {
                logger.info("[" + server.getServerId() + "]: updated [" + id + "]’s heartbeat sequence to [" + lastHeartbeat + "] based on message from [" + senderID + "] at node time [" + System.currentTimeMillis() + "]");
                if(lastHeartbeat > heartbeatCounter) heartbeatCounter = lastHeartbeat;
                idToLastHeartbeat.put(id, lastHeartbeat);
                idToLastHeartbeatTime.put(id, currentTime);
            }
            else if(currentTime - idToLastHeartbeatTime.getOrDefault(id, 0L) >= FAIL) {
                reportAsDead(id);
                logger.warning("[Server " + server.getServerId() + "]: No heartbeat from server [" + id + "] – server FAILED");
                System.out.println("[Server " + server.getServerId() + "]: No heartbeat from server [" + id + "] – server FAILED");
                new Thread(() -> {
                    try {
                        Thread.sleep(CLEANUP);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    idToLastHeartbeat.remove(id);
                    idToLastHeartbeatTime.remove(id);
                }).start();
            }
        }
        logger.info("Updated heartbeat data sent from server ID " + senderID);
    }

    public boolean isPeerDead(long id) {
        return failedServersIDs.contains(id);
    }

    public void reportAsDead(long id) {
        failedServersIDs.add(id);
        if(server.peerIDtoAddress.containsKey(id)) failedServersAddresses.add(server.peerIDtoAddress.get(id));
        server.reportFailedPeer(id);
    }

    public boolean isPeerDead(InetSocketAddress id) {
        return failedServersAddresses.contains(id);
    }

    private byte[] createGossipMessageContents() {
        int allocationSize = 8 + idToLastHeartbeat.size() * 16;
        ByteBuffer byteBuffer = ByteBuffer.allocate(allocationSize); // id + map
        byteBuffer.putLong(server.getServerId());
        int bytesReceived = 8;
        try {
            for (Long id : idToLastHeartbeat.keySet()) {
                if (bytesReceived >= allocationSize) break;
                byteBuffer.putLong(id);
                byteBuffer.putLong(idToLastHeartbeat.getOrDefault(id, 0L));
                bytesReceived += 16;
            }
        }
        catch (BufferOverflowException e) {
            logger.warning("Caused BufferOverflowException because size of map grew since allocation. This is a normal occurrence, not an issue.");
        }
        byteBuffer.flip();
        return byteBuffer.array();
    }

    public List<GossipMessageInfo> getListOfReceivedGossips() {
        return listOfReceivedGossips;
    }

    private class ReceivingHeartbeats extends Thread {
        private LinkedBlockingQueue<Message> incomingHeartbeats;

        public ReceivingHeartbeats(LinkedBlockingQueue<Message> incomingHeartbeats) {
            this.incomingHeartbeats = incomingHeartbeats;
        }

        public void shutdown() {
            interrupt();
        }

        @Override
        public void run() {
            while(!shutdown) {
                Message beat = incomingHeartbeats.poll();
                if(beat != null) {
                    updatePeerHeartbeatStatuses(beat);
                }
            }
        }
    }

    public static class GossipMessageInfo {
        long senderID;
        byte[] messageContents;
        long timeReceived;

        public GossipMessageInfo(long senderID, byte[] messageContents, long timeReceived) {
            this.senderID = senderID;
            this.messageContents = messageContents;
            this.timeReceived = timeReceived;
        }

        @Override
        public String toString() {
            return "\nReceived GOSSIP message from server " + senderID + " at time " + timeReceived + ".\nContents: " + new String(messageContents);
        }
    }
}

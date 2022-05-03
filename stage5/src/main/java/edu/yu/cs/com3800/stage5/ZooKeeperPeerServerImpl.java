package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import jdk.swing.interop.SwingInterOpUtils;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    final InetSocketAddress myAddress;
    int myPort;
    volatile ServerState state;
    volatile boolean shutdown = false;
    LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Message> incomingHeartbeats = new LinkedBlockingQueue<>();
    Long id;
    long peerEpoch;
    volatile Vote currentLeader;
    Map<Long,InetSocketAddress> peerIDtoAddress;
    volatile int nObservers;
    Logger logger;
    UDPMessageSender senderWorker;
    UDPMessageReceiver receiverWorker;
    JavaRunnerFollower runner;
    LinkedBlockingQueue<Message> toRunner;
    LinkedBlockingQueue<Message> runnerResponses;
    RoundRobinLeader roundRobinLeader;
    Message queuedUpRunnerWork;
    HeartbeatMonitor heartbeatMonitor;
    volatile static InetSocketAddress gatewayServerAddress = null;

    public static void main(String[] args) {
        ConcurrentHashMap<Long,InetSocketAddress> map = new ConcurrentHashMap<>();
        int port = 8010;
        int myPort = -1;
        long myId = Integer.parseInt(args[0]);
        for (long i = 1; i <= 7; i++) {
            if(i == myId){
                myPort = port;
            }
            map.put(i, new InetSocketAddress("localhost", port));
            port += 10;
        }
        map.put(8L, new InetSocketAddress("localhost", port + 5));
        ZooKeeperPeerServerImpl zooKeeperPeerServer = new ZooKeeperPeerServerImpl(myPort, 0, myId, map, 1);
        zooKeeperPeerServer.start();
    }

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress, int nObservers) {
        this.myPort = myPort;
        this.peerEpoch = peerEpoch;
        this.id = id;
        this.peerIDtoAddress = peerIDtoAddress;
        this.myAddress = new InetSocketAddress("localhost", this.myPort);
        if(peerIDtoAddress.get(this.id) == null) {
            this.peerIDtoAddress.put(this.id, this.myAddress);
        }
        this.state = ServerState.LOOKING;
        this.currentLeader = new Vote(this.id, this.peerEpoch);
        try {
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages, this.incomingHeartbeats, this.myAddress,this.myPort,this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.senderWorker = new UDPMessageSender(this.outgoingMessages,this.myPort);
        try {
            this.logger = initializeLogging(Util.getLoggerName(ZooKeeperPeerServerImpl.class.getSimpleName(), id, myPort));
            this.logger.info("Created server");
        } catch (IOException e) {
            e.printStackTrace();
        }
        this.toRunner = new LinkedBlockingQueue<>();
        this.runnerResponses = new LinkedBlockingQueue<>();
        this.nObservers = nObservers;
        try {
            this.heartbeatMonitor = new HeartbeatMonitor(this, this.incomingHeartbeats);
            Util.startAsDaemon(this.heartbeatMonitor, "HeartbeatMonitor on server " + this.id);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown(){
        logger.severe("SHUTTING DOWN");
        this.shutdown = true;
//        System.out.println("Shutting down server ID " + getServerId());
        this.heartbeatMonitor.shutdown();
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
        if(this.roundRobinLeader != null) this.roundRobinLeader.shutdown();
        if(this.runner != null) this.runner.shutdown();
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        assert target != null;
        outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(), target.getPort()));
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress target : peerIDtoAddress.values()) {
            if(target.equals(this.myAddress)) continue;
            outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(), target.getPort()));
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        logger.info("[" + id + "]: switching from [" + this.state + "] to [" + newState + "]");
        System.out.println("[" + id + "]: switching from [" + this.state + "] to [" + newState + "]");
        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    @Override
    public int getQuorumSize() {
        return peerIDtoAddress.keySet().size() - this.nObservers;
    }

    @Override
    public void reportFailedPeer(long peerID) {
        if(peerID == getServerId()) {
            shutdown();
        }
        else if(getCurrentLeader().getProposedLeaderID() == peerID) {
            if(runner != null) runner.shutdown();
            this.peerEpoch++;
            setPeerState(ServerState.LOOKING);
            try {
                setCurrentLeader(new Vote(getServerId(), getPeerEpoch()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public boolean isPeerDead(long peerID) {
        return heartbeatMonitor.isPeerDead(peerID);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return heartbeatMonitor.isPeerDead(address);
    }

    @Override
    public void run() {
        Util.startAsDaemon(senderWorker, "UDPMessageSender");
        Util.startAsDaemon(receiverWorker, "UDPMessageReceiver");

        while(!this.isInterrupted() && !this.shutdown) {
            try {
                switch(getPeerState()) {
                    case LOOKING:
                        logger.warning("here1");
                        setCurrentLeader(startLeaderElection());
                        break;
                    case FOLLOWING:
                        logger.info("We're following the leader again");
                        Queue<Message> oldQueuedUpWork;
                        if(runner != null) {
                            oldQueuedUpWork = runner.queuedUpWork;
                        }
                        else oldQueuedUpWork = new LinkedList<>();
                        runner = new JavaRunnerFollower(this, oldQueuedUpWork, logger);
                        Util.startAsDaemon(runner, myAddress.toString() + ": javarunner");
                        while(!isInterrupted() && getPeerState() == ServerState.FOLLOWING && !shutdown) Thread.onSpinWait();
                        logger.warning("here");
                        break;
                    case LEADING:
                        Queue<Message> oldQueuedUpWork1;
                        if(runner != null) {
                            oldQueuedUpWork1 = runner.queuedUpWork;
                            runner.shutdown();
                            runner = null;
                        }
                        else oldQueuedUpWork1 = new LinkedList<>();
//                        System.out.println("Gatewaypeerserveraddress: " + gatewayServerAddress);
                        roundRobinLeader = new RoundRobinLeader(this, incomingMessages, new ArrayList<>(peerIDtoAddress.keySet()), oldQueuedUpWork1, gatewayServerAddress, logger);
                        Util.startAsDaemon(roundRobinLeader, myAddress.toString() + ": roundrobin");
                        while(!this.isInterrupted() && getPeerState() == ServerState.LEADING && !shutdown) Thread.onSpinWait();
                        if(!shutdown && !isInterrupted()) {
                            roundRobinLeader.shutdown();
                        }
                        logger.warning("here2");
                        break;
                    case OBSERVER:
                        if(this.currentLeader.getProposedLeaderID() == this.id || isPeerDead(this.currentLeader.getProposedLeaderID())) {
                            setCurrentLeader(startLeaderElection());
                        }
                        while(!isInterrupted() && !shutdown && !isPeerDead(this.currentLeader.getProposedLeaderID()) && this.currentLeader.getProposedLeaderID() != this.id) Thread.onSpinWait();
                        if(shutdown || isInterrupted()) break;
                        setCurrentLeader(startLeaderElection());
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                return;
            }
        }
        if(!shutdown) shutdown();
    }

    public static void setGatewayServerAddress(InetSocketAddress inetSocketAddress) {
        gatewayServerAddress = inetSocketAddress;
    }

    protected Vote startLeaderElection() {
        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, this.incomingMessages);
        logger.info("Initiating leader election");
        Vote leader = null;
        try {
            leader = election.lookForLeader();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assert leader != null;
        logger.info("Elected server with ID " + leader.getProposedLeaderID() + " as leader");
        return leader;
    }

    public Logger getLogger() { // todo: rid this
        return logger;
    }

    boolean isShutdown() {
        return shutdown;
    }

}



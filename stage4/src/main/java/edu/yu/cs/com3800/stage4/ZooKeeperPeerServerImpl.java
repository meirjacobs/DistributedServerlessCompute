package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    final InetSocketAddress myAddress;
    int myPort;
    ServerState state;
    volatile boolean shutdown = false;
    LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
    Long id;
    long peerEpoch;
    volatile Vote currentLeader;
    Map<Long,InetSocketAddress> peerIDtoAddress;
    int nObservers;
    Logger logger;
    UDPMessageSender senderWorker;
    UDPMessageReceiver receiverWorker;
    JavaRunnerFollower runner;
    LinkedBlockingQueue<Message> toRunner;
    LinkedBlockingQueue<Message> runnerResponses;
    RoundRobinLeader roundRobinLeader;

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
            this.receiverWorker = new UDPMessageReceiver(this.incomingMessages,this.myAddress,this.myPort,this);
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
    }

    @Override
    public void shutdown(){
        this.shutdown = true;
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

    }

    @Override
    public boolean isPeerDead(long peerID) {
        return false;
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        return false;
    }

    @Override
    public void run() {
        logger.info("here1");
        Util.startAsDaemon(senderWorker, "UDPMessageSender");
        Util.startAsDaemon(receiverWorker, "UDPMessageReceiver");

        while(!this.shutdown) {
            logger.info("here2");
            try {
                switch(getPeerState()) {
                    case LOOKING:
                        setCurrentLeader(startLeaderElection());
                        break;
                    case FOLLOWING:
                        runner = new JavaRunnerFollower(this, logger);
                        Util.startAsDaemon(runner, myAddress.toString() + ": javarunner");
                        while(getPeerState() == ServerState.FOLLOWING && !shutdown) Thread.onSpinWait();
                        break;
                    case LEADING:
                        roundRobinLeader = new RoundRobinLeader(this, incomingMessages, new ArrayList<>(peerIDtoAddress.keySet()), logger);
                        Util.startAsDaemon(roundRobinLeader, myAddress.toString() + ": roundrobin");
                        while(getPeerState() == ServerState.LEADING && !shutdown) Thread.onSpinWait();
                        break;
                    case OBSERVER:
                        logger.info("Got here");
                        if(this.currentLeader.getProposedLeaderID() == this.id || isPeerDead(this.currentLeader.getProposedLeaderID())) {
                            setCurrentLeader(startLeaderElection());
                        }
                        while(true);
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
    }

    Vote startLeaderElection() {
        ZooKeeperLeaderElection election = new ZooKeeperLeaderElection(this, this.incomingMessages);
        logger.info("Initiating leader election");
        Vote leader = election.lookForLeader();
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



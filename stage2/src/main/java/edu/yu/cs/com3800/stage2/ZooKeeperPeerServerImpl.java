package edu.yu.cs.com3800.stage2;

import edu.yu.cs.com3800.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;


public class ZooKeeperPeerServerImpl extends Thread implements ZooKeeperPeerServer {
    private final InetSocketAddress myAddress;
    private int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages = new LinkedBlockingQueue<>();
    private LinkedBlockingQueue<Message> incomingMessages = new LinkedBlockingQueue<>();
    private Long id;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long,InetSocketAddress> peerIDtoAddress;
    private Logger logger;

    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;

    public ZooKeeperPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long,InetSocketAddress> peerIDtoAddress){
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
            this.logger = initializeLogging(ZooKeeperPeerServerImpl.class.getCanonicalName() + "-on-port-" + this.myPort);
            this.logger.info("Created server");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void shutdown(){
        this.shutdown = true;
        this.senderWorker.shutdown();
        this.receiverWorker.shutdown();
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
        boolean inQueue = outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(), target.getPort()));
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for(InetSocketAddress target : peerIDtoAddress.values()) {
            if(target.equals(this.myAddress)) continue;
            boolean inQueue = outgoingMessages.offer(new Message(type, messageContents, myAddress.getHostName(), myPort, target.getHostName(), target.getPort()));
            if(!inQueue) {
                int i = 1;
            }
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
        return peerIDtoAddress.keySet().size();
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
    public void run(){
        //step 1: create and run thread that sends broadcast messages
        this.senderWorker.start();
        //step 2: create and run thread that listens for messages sent to this server
        this.receiverWorker.start();

        //step 3: process received messages
        while(!this.shutdown) {
            try {
                switch(getPeerState()) {
                    case LOOKING:
                        //start leader election, set leader to the election winner
                        setCurrentLeader(startLeaderElection());
                        break;
                    default:
                        break;
                }
//                Message msg = this.incomingMessages.take();
//                System.out.println("@" + myAddress.getPort() + ": RECEIVED message from client at " + msg.getSenderHost() +
//                        ":" + msg.getSenderPort() + ". Message: " + new String(msg.getMessageContents()));
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
        return election.lookForLeader();
    }

}

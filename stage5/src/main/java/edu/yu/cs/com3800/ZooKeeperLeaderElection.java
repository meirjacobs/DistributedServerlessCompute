package edu.yu.cs.com3800;

import edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static edu.yu.cs.com3800.ZooKeeperPeerServer.ServerState.*;

public class ZooKeeperLeaderElection
{
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 60 seconds.
     */
    private final static int maxNotificationInterval = 60000;

    private LinkedBlockingQueue<Message> incomingMessages;
    private ZooKeeperPeerServer myPeerServer;
    private long proposedLeader;
    private long proposedEpoch;
    private Map<Long, Long> serversAndTheirVotes;
    private Map<Long, Integer> leaderVotes;
    private int totalVotes;
    ZooKeeperPeerServerImpl server;

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages)
    {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.server = (ZooKeeperPeerServerImpl) this.myPeerServer;
        this.proposedLeader = myPeerServer.getCurrentLeader().getProposedLeaderID();
        this.proposedEpoch = myPeerServer.getPeerEpoch();
        this.serversAndTheirVotes = new HashMap<>();
        this.leaderVotes = new HashMap<>();
        this.totalVotes = 0;
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader() throws InterruptedException {
        Map<Long, ElectionNotification> votes = new HashMap<>();
        sendNotifications();
        while (this.myPeerServer.getPeerState() == LOOKING ||
                (this.myPeerServer.getPeerState() == OBSERVER &&
                        this.myPeerServer.getCurrentLeader().getProposedLeaderID() == this.myPeerServer.getServerId())) {
            int timeout = finalizeWait;
            Message message = null;
            ElectionNotification electionNotification = null;
            while(true) {
                try {
                    message = incomingMessages.poll(timeout, TimeUnit.MILLISECONDS);
                    server.getLogger().info("Message: " + (message != null ? message.toString() : "null"));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(message != null && message.getMessageType() == Message.MessageType.ELECTION) {
                    electionNotification = getNotificationFromMessage(message);
                    if(validServersInNotification(electionNotification)) break;
                    else electionNotification = null;
                    server.getLogger().info("Invalid server in notification");
                    Thread.sleep(100);
                    message = null;
                }
                sendNotifications();
                int tmpTimeOut = timeout * 2;
                timeout = Math.min(tmpTimeOut, maxNotificationInterval);
                server.getLogger().info("Timeout = " + timeout);
            }
            votes.put(electionNotification.getSenderID(), electionNotification);
            switch (electionNotification.getState()) {
                case LOOKING:
                    server.getLogger().info("Received LOOKING notification from " + electionNotification.getSenderID());
                    if (supersedesCurrentVote(electionNotification.getProposedLeaderID(), electionNotification.getPeerEpoch()) || (myPeerServer.getPeerState() == OBSERVER && this.proposedLeader == myPeerServer.getServerId())) {
                        server.getLogger().info("Vote supersedes current vote");
                        updateVote(electionNotification);
                        server.getLogger().info("Sending broadcast of new vote");
                        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(new ElectionNotification(electionNotification.getProposedLeaderID(), myPeerServer.getPeerState(), myPeerServer.getServerId(), proposedEpoch)));
                    }
                    leaderVotes.put(electionNotification.getProposedLeaderID(), leaderVotes.getOrDefault(electionNotification.getProposedLeaderID(), 0) + 1);
                    if(haveEnoughVotes(votes, getCurrentVote()) && !newVoteForHigherRankedServer()) {
//                        System.out.println("dis0");
                        return acceptElectionWinner(new ElectionNotification(getCurrentVote().getProposedLeaderID(), LOOKING, myPeerServer.getServerId(), this.proposedEpoch));
                    }
                    break;
                case FOLLOWING:
                case LEADING:
                    server.getLogger().info("Received FOLLOWING/LEADING notification from " + electionNotification.getSenderID());
                    if(electionNotification.getPeerEpoch() == this.proposedEpoch && haveEnoughVotes(votes, electionNotification)) {
//                        System.out.println("dis2");
                        return acceptElectionWinner(electionNotification);
                    }
                    else {
                        if(electionNotification.getPeerEpoch() > this.proposedEpoch) {
                            if(haveEnoughVotes(votes, electionNotification)) {
//                                System.out.println("dis");
                                return acceptElectionWinner(electionNotification);
                            }
                        }
                    }
                    break;
                case OBSERVER:
                    server.getLogger().info("Received OBSERVER notification from " + electionNotification.getSenderID());
                    continue;
            }
        }
//        System.out.println("Got to here ;(");
        return null; // Should never reach this statement because we should never exit the while loop unless returning from the method
    }

    private Vote acceptElectionWinner(ElectionNotification n)
    {
        if(getCurrentVote().getProposedLeaderID() != n.getProposedLeaderID()) {
            updateVote(n);
        }
        if(n.getProposedLeaderID() == myPeerServer.getServerId()) {
            myPeerServer.setPeerState(LEADING);
            sendNotifications();
        }
        else {
            myPeerServer.setPeerState(FOLLOWING);
        }
        if(myPeerServer.getPeerState() == OBSERVER) {
            waitForLeaderToKnowItsLeading(n.getProposedLeaderID());
        }
        try {
            myPeerServer.setCurrentLeader(n);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getCurrentVote();
    }

    private void waitForLeaderToKnowItsLeading(long proposedLeaderID) {
        long start = System.currentTimeMillis();
        while(System.currentTimeMillis() - start < 15000) {
            Message message = incomingMessages.poll();
            if(message == null) continue;
            ElectionNotification electionNotification = getNotificationFromMessage(message);
            if(electionNotification.getState() == LEADING && electionNotification.getSenderID() == proposedLeaderID) {
                return;
            }
        }
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
     protected boolean supersedesCurrentVote(long newId, long newEpoch) {
         return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
     }
    /**
     * Termination predicate. Given a set of votes, determines if have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) // this is super inefficient and probably not what he wants
    {
       //is the number of votes for the proposal > the size of my peer server’s quorum?
        long id = proposal.getProposedLeaderID();
        long epoch = proposal.getPeerEpoch();
        int totVotes = 0;
        for(ElectionNotification vote : votes.values()) {
            if(vote.getPeerEpoch() == proposal.getPeerEpoch()) {
                if(vote.getProposedLeaderID() == id) {
                    totVotes++;
                }
            }
        }
        if(totVotes > myPeerServer.getQuorumSize() / 2) {
            return true;
        }
        return false;
    }

    static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(26); // 3 longs and a char = 24 + 2
        byteBuffer.putLong(notification.getProposedLeaderID());
        byteBuffer.putChar(notification.getState().getChar());
        byteBuffer.putLong(notification.getSenderID());
        byteBuffer.putLong(notification.getPeerEpoch());
        byteBuffer.flip();
        return byteBuffer.array();
    }

    private void sendNotifications() {
        server.getLogger().info("Sending notifications");
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(new ElectionNotification(proposedLeader, myPeerServer.getPeerState(), myPeerServer.getServerId(), myPeerServer.getPeerEpoch())));
    }

    private boolean validServersInNotification(ElectionNotification notification) {
        return !myPeerServer.isPeerDead(notification.getSenderID()) && !myPeerServer.isPeerDead(notification.getProposedLeaderID());
    }

    private boolean newVoteForHigherRankedServer() {
        Message m;
        while((m = receiveMessage(finalizeWait, TimeUnit.MILLISECONDS)) != null) {
            ElectionNotification n = getNotificationFromMessage(m);
            if(supersedesCurrentVote(n.getProposedLeaderID(), n.getPeerEpoch())) {
                try {
                    incomingMessages.put(m);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return true;
            }
        }
        return false;
    }

    private Message receiveMessage(int finalizeWait, TimeUnit timeUnit) {
        Message message = null;
        try {
            message = incomingMessages.poll(finalizeWait, timeUnit);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return message;
    }

    protected static ElectionNotification getNotificationFromMessage(Message message) {
        ByteBuffer msgBytes = ByteBuffer.wrap(message.getMessageContents());
        long leader = msgBytes.getLong();
        char stateChar = msgBytes.getChar();
        long senderID = msgBytes.getLong();
        long peerEpoch = msgBytes.getLong();
        return new ElectionNotification(leader, ZooKeeperPeerServer.ServerState.getServerState(stateChar), senderID, peerEpoch);
    }

    private void updateVote(Vote vote) {
        server.getLogger().info("Updating vote to " + vote.getProposedLeaderID());
        this.proposedEpoch = vote.getPeerEpoch();
        this.proposedLeader = vote.getProposedLeaderID();
    }
}

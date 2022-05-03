package edu.yu.cs.com3800;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
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

    public ZooKeeperLeaderElection(ZooKeeperPeerServer server, LinkedBlockingQueue<Message> incomingMessages)
    {
        this.incomingMessages = incomingMessages;
        this.myPeerServer = server;
        this.proposedLeader = myPeerServer.getCurrentLeader().getProposedLeaderID();
        this.proposedEpoch = myPeerServer.getPeerEpoch();
        this.serversAndTheirVotes = new HashMap<>();
        this.leaderVotes = new HashMap<>();
        this.totalVotes = 0;
    }

    private synchronized Vote getCurrentVote() {
        return new Vote(this.proposedLeader, this.proposedEpoch);
    }

    public synchronized Vote lookForLeader()
    {
        Map<Long, ElectionNotification> votes = new HashMap<>();
        //send initial notifications to other peers to get things started
        sendNotifications();
        //Loop, exchanging notifications with other servers until we find a leader
        while (this.myPeerServer.getPeerState() == LOOKING) {
            //Remove next notification from queue, timing out after 2 times the termination time
            //if no notifications received..
            //..resend notifications to prompt a reply from others..
            //.and implement exponential back-off when notifications not received..
            //if/when we get a message and it's from a valid server and for a valid server..
//            Message message = null;
//            ElectionNotification electionNotification = null;
//            int multiplier = 1;
//            while(true) {
//                message = lookingForMessage(multiplier);
//                if(message != null) {
//                    electionNotification = getNotificationFromMessage(message);
//                    if(validServersInNotification(electionNotification)) break;
//                    else electionNotification = null;
//                }
//                multiplier *= 2;
//                sendNotifications();
//            }
            int timeout = finalizeWait;
            Message message = null;
            ElectionNotification electionNotification = null;
            while(true) {
                try {
                    message = incomingMessages.poll(timeout, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(message != null) {
                    electionNotification = getNotificationFromMessage(message);
                    if(validServersInNotification(electionNotification)) break;
                    else electionNotification = null;
                    message = null;
                }
                sendNotifications();
                int tmpTimeOut = timeout * 2;
                timeout = Math.min(tmpTimeOut, maxNotificationInterval);
            }
            //switch on the state of the sender:
            votes.put(electionNotification.getSenderID(), electionNotification);
            //System.out.printf("Server %d received vote for %d\n", myPeerServer.getServerId(), electionNotification.getProposedLeaderID());
            if(electionNotification == null) {
                int a = 4;
            }
            switch (electionNotification.getState()) {
                case LOOKING: //if the sender is also looking
                    //if the received message has a vote for a leader which supersedes mine, change my vote and tell all my peers what my new vote is.
                    if (supersedesCurrentVote(electionNotification.getProposedLeaderID(), electionNotification.getPeerEpoch())) {
                        updateVote(electionNotification);
                        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(new ElectionNotification(electionNotification.getProposedLeaderID(), LOOKING, myPeerServer.getServerId(), proposedEpoch)));
                    }
                    //keep track of the votes I received and who I received them from.
                    leaderVotes.put(electionNotification.getProposedLeaderID(), leaderVotes.getOrDefault(electionNotification.getProposedLeaderID(), 0) + 1);
                    ////if I have enough votes to declare my currently proposed leader as the leader:
                    //first check if there are any new votes for a higher ranked possible leader before I declare a leader. If so, continue in my election loop
                    //If not, set my own state to either LEADING (if I won the election) or FOLLOWING (if someone else won the election) and exit the election
                    if(haveEnoughVotes(votes, getCurrentVote()) && !newVoteForHigherRankedServer()) {
                        return acceptElectionWinner(new ElectionNotification(getCurrentVote().getProposedLeaderID(), LOOKING, myPeerServer.getServerId(), this.proposedEpoch));
                    }
                    break;
                case FOLLOWING:
                case LEADING: //if the sender is following a leader already or thinks it is the leader
                    //System.out.printf("Server id %d encounters %s from server id %d\n", myPeerServer.getServerId(), electionNotification.getState().toString(), electionNotification.getProposedLeaderID());
                    //IF: see if the sender's vote allows me to reach a conclusion based on the election epoch that I'm in, i.e. it gives the majority to the vote of the FOLLOWING or LEADING peer whose vote I just received.
                    //if so, accept the election winner.
                    //As, once someone declares a winner, we are done. We are not worried about / accounting for misbehaving peers.
                    if(electionNotification.getPeerEpoch() == this.proposedEpoch && haveEnoughVotes(votes, electionNotification)) {
                        return acceptElectionWinner(electionNotification);
                    }
                    //ELSE:
                    else {
                        //if n is from a LATER election epoch
                        if(electionNotification.getPeerEpoch() > this.proposedEpoch) {
                            //IF a quorum from that epoch are voting for the same peer as the vote of the FOLLOWING or LEADING peer whose vote I just received.
                            //THEN accept their leader, and update my epoch to be their epoch
                            if(haveEnoughVotes(votes, electionNotification)) {
                                return acceptElectionWinner(electionNotification);
                            }
                            //ELSE:
                            //keep looping on the election loop.
                        }
                    }
            }
        }
        return null; // Should never reach this statement because we should never exit the while loop unless returning from the method
    }

    private Vote acceptElectionWinner(ElectionNotification n)
    {
        //set my state to either LEADING or FOLLOWING
        //clear out the incoming queue before returning
        //System.out.println(myPeerServer.getServerId() + " accepted " + n.getProposedLeaderID() + " as leader");
        if(getCurrentVote().getProposedLeaderID() != n.getProposedLeaderID()) {
            updateVote(n);
        }
        if(n.getProposedLeaderID() == myPeerServer.getServerId()) {
            myPeerServer.setPeerState(LEADING);
        }
        else {
            myPeerServer.setPeerState(FOLLOWING);
        }
        incomingMessages.clear();
        try {
            myPeerServer.setCurrentLeader(n);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return getCurrentVote();
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
     protected boolean supersedesCurrentVote(long newId, long newEpoch) {
         return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
         //return newId > this.proposedLeader;
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
            //System.out.println(id + " has " + totVotes + " votes");
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
        myPeerServer.sendBroadcast(Message.MessageType.ELECTION, buildMsgContent(new ElectionNotification(proposedLeader, myPeerServer.getPeerState(), myPeerServer.getServerId(), myPeerServer.getPeerEpoch())));
    }

    private Message lookingForMessage(int timeoutMultiplier) {
        Message message = null;
        long startTime = System.currentTimeMillis();
        while(System.currentTimeMillis() - startTime < timeoutMultiplier * maxNotificationInterval) {
            message = incomingMessages.poll();
            if(message != null) {
                break;
            }
        }
        return message;
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
        //System.out.printf("Server id %d updating leader from %d to %d\n", myPeerServer.getServerId(), this.proposedLeader, vote.getProposedLeaderID());
        this.proposedEpoch = vote.getPeerEpoch();
        this.proposedLeader = vote.getProposedLeaderID();
    }
}

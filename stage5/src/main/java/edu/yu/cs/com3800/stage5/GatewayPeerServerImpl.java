package edu.yu.cs.com3800.stage5;

import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, int nObservers) {
        super(myPort, peerEpoch, id, peerIDtoAddress, nObservers);
        this.state = ServerState.OBSERVER;
    }

    @Override
    public void setPeerState(ServerState newState) {}

    public String getNodeIDs() {
//        System.out.println("Entered method");
        while(getCurrentLeader() == null || getCurrentLeader().getProposedLeaderID() == this.id) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            System.out.println("Waiting");
        }
        StringBuilder builder = new StringBuilder();
        builder.append(getCurrentLeader().getProposedLeaderID());
        for(long l : peerIDtoAddress.keySet()) {
            if(l != getCurrentLeader().getProposedLeaderID() && l != this.id && !heartbeatMonitor.isPeerDead(l)) {
                builder.append(l);
            }
        }
        return builder.toString();
    }

    public void waitUntilNodeIsDead(long id) {
        while(!heartbeatMonitor.isPeerDead(id)) {
            try {
                Thread.sleep(20);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

}

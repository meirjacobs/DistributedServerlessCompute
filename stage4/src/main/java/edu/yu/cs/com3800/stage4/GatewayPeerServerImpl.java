package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;

public class GatewayPeerServerImpl extends ZooKeeperPeerServerImpl {

    public GatewayPeerServerImpl(int myPort, long peerEpoch, Long id, Map<Long, InetSocketAddress> peerIDtoAddress, int nObservers) {
        super(myPort, peerEpoch, id, peerIDtoAddress, nObservers);
        this.state = ServerState.OBSERVER;
    }

    @Override
    public void setPeerState(ServerState newState) {}

}

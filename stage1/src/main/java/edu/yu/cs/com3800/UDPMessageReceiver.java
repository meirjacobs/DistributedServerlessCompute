//package edu.yu.cs.com3800;
//
//import java.io.IOException;
//import java.net.*;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.logging.Level;
//import java.util.logging.Logger;
//
//public class UDPMessageReceiver extends Thread implements LoggingServer {
//    private static final int MAXLENGTH = 4096;
//    private final InetSocketAddress myAddress;
//    private final int myPort;
//    private LinkedBlockingQueue<Message> incomingMessages;
//    private Logger logger;
//    private ZooKeeperPeerServer peerServer;
//
//    public UDPMessageReceiver(LinkedBlockingQueue<Message> incomingMessages, InetSocketAddress myAddress, int myPort, ZooKeeperPeerServer peerServer) throws IOException {
//        this.incomingMessages = incomingMessages;
//        this.myAddress = myAddress;
//        this.myPort = myPort;
//        this.logger = initializeLogging(UDPMessageReceiver.class.getCanonicalName() + "-on-port-" + this.myPort, true);
//        this.setDaemon(true);
//        this.peerServer = peerServer;
//        setName("UDPMessageReceiver-port-" + this.myPort);
//    }
//
//    public void shutdown() {
//        interrupt();
//    }
//
//    @Override
//    public void run() {
//        //create the socket
//        DatagramSocket socket = null;
//        try {
//            socket = new DatagramSocket(this.myAddress);
//            socket.setSoTimeout(3000);
//        }
//        catch (Exception e) {
//            this.logger.log(Level.SEVERE, "failed to create receiving socket", e);
//            return;
//        }
//        //loop
//        while (!this.isInterrupted()) {
//            try {
//                this.logger.fine("Waiting for packet");
//                DatagramPacket packet = new DatagramPacket(new byte[MAXLENGTH], MAXLENGTH);
//                socket.receive(packet); // Receive packet from a client
//                InetSocketAddress sender = new InetSocketAddress(packet.getAddress(), packet.getPort());
//                //ignore messages from peers marked as dead
//                if (this.peerServer != null && this.peerServer.isPeerDead(sender)) {
//                    this.logger.fine("UDP packet received from dead peer: " + sender.toString() + "; ignoring it.");
//                    continue;
//                }
//                Message received = new Message(packet.getData());
//                this.logger.fine("UDP packet received:\n" + received.toString());
//                //this is logic required for stage 5...
//                if (sendLeader(received)) {
//                    Vote leader = this.peerServer.getCurrentLeader();
//                    ElectionNotification notification = new ElectionNotification(leader.getProposedLeaderID(), this.peerServer.getPeerState(), this.peerServer.getServerId(), this.peerServer.getPeerEpoch());
//                    byte[] payload = ZooKeeperLeaderElection.buildMsgPayload(notification);
//                    sendElectionReply(payload, sender);
//                //end stage 5 logic
//                }
//                else {
//                    this.incomingMessages.put(received);
//                }
//            }
//            catch (SocketTimeoutException ste) {
//            }
//            catch (Exception e) {
//                if (!this.isInterrupted()) {
//                    this.logger.log(Level.WARNING, "Exception caught while trying to receive UDP packet", e);
//                }
//            }
//        }
//        //cleanup
//        if (socket != null) {
//            socket.close();
//        }
//    }
//
//    private void sendElectionReply(byte[] payload, InetSocketAddress target) {
//        Message msg = new Message(Message.MessageType.ELECTION, payload, this.myAddress.getHostString(), this.myPort, target.getHostString(), target.getPort());
//        try (DatagramSocket socket = new DatagramSocket()){
//            DatagramPacket sendPacket = new DatagramPacket(payload, payload.length, target);
//            socket.send(sendPacket);
//            this.logger.fine("Election reply sent:\n" + msg.toString());
//        }
//        catch (IOException e) {
//            this.logger.warning("Failed to send election reply:\n" + msg.toString());
//        }
//    }
//
//    /**
//     * see if we got an Election LOOKING message while we are in FOLLOWING or LEADING
//     * @param received
//     * @return
//     */
//    private boolean sendLeader(Message received) {
//        if (received.getMessageType() != Message.MessageType.ELECTION) {
//            return false;
//        }
//        ElectionNotification receivedNotification = ZooKeeperLeaderElection.getNotificationFromMessage(received);
//        if (receivedNotification.getState() == ZooKeeperPeerServer.ServerState.LOOKING && (this.peerServer.getPeerState() == ZooKeeperPeerServer.ServerState.FOLLOWING || this.peerServer.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING)) {
//            return true;
//        }
//        else {
//            return false;
//        }
//    }
//}
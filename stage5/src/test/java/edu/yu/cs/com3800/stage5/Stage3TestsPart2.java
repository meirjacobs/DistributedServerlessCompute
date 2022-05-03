//package edu.yu.cs.com3800.stage5;
//
//import edu.yu.cs.com3800.*;
//import org.junit.jupiter.api.Test;
//
//import java.net.InetSocketAddress;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class Stage3TestsPart2 {
//    private String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
//
//    private LinkedBlockingQueue<Message> outgoingMessages;
//    private LinkedBlockingQueue<Message> incomingMessages;
//    private int[] ports = {8010, 8020, 8030, 8040, 8050, 8060, 8070, 8080};
//    //private int[] ports = {8010, 8020};
//    private int leaderPort = this.ports[this.ports.length - 1];
//    private int myPort = 9989;
//    private InetSocketAddress myAddress = new InetSocketAddress("localhost", this.myPort);
//    private ArrayList<ZooKeeperPeerServer> servers;
//
//    /*@AfterEach
//    public void reset() throws InterruptedException {
//        System.out.println("\n\nresetting\n\n");
//        outgoingMessages = incomingMessages = null;
//        servers = null;
//        for(int i = 0; i < ports.length; i++) {
//            ports[i] = ports[i] + 100;
//        }
//        myPort++;
//        myAddress = new InetSocketAddress("localhost", myPort);
//        leaderPort = this.ports[this.ports.length - 1];
//        Thread.sleep(5000);
//    }*/
//
//    @Test
//    public void Stage3PeerServerDemo() throws Exception {
//        System.out.println("\n\nresetting\n\n");
//        outgoingMessages = incomingMessages = null;
//        servers = null;
//        for(int i = 0; i < ports.length; i++) {
//            ports[i] = ports[i] + 101;
//        }
//        myPort += 2;
//        myAddress = new InetSocketAddress("localhost", myPort);
//        leaderPort = this.ports[this.ports.length - 1];
//        Thread.sleep(5000);
//
//
//        //step 1: create sender & sending queue
//        this.outgoingMessages = new LinkedBlockingQueue<>();
//        UDPMessageSender sender = new UDPMessageSender(this.outgoingMessages, this.myPort);
//        //step 2: create servers
//        createServers();
//        //step2.1: wait for servers to get started
//        try {
//            Thread.sleep(3000);
//        }
//        catch (Exception e) {
//        }
//        printLeaders();
//        //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
//        for (int i = 0; i < this.ports.length; i++) {
//            String code = this.validClass.replace("world!", "world! from code version " + i);
//            sendMessage(code);
//        }
//        Util.startAsDaemon(sender, "Sender thread");
//        this.incomingMessages = new LinkedBlockingQueue<>();
//        UDPMessageReceiver receiver = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, null);
//        Util.startAsDaemon(receiver, "Receiver thread");
//        //step 4: validate responses from leader
//        printResponses();
//
//        //step 5: stop servers
//        stopServers();
//    }
//
//    @Test
//    public void Stage3PeerServerDemoInvalidCode() throws Exception {
//        System.out.println("\n\nresetting\n\n");
//        outgoingMessages = incomingMessages = null;
//        servers = null;
//        for(int i = 0; i < ports.length; i++) {
//            ports[i] = ports[i] + 100;
//        }
//        myPort++;
//        myAddress = new InetSocketAddress("localhost", myPort);
//        leaderPort = this.ports[this.ports.length - 1];
//        Thread.sleep(5000);
//
//
//        //step 1: create sender & sending queue
//        this.outgoingMessages = new LinkedBlockingQueue<>();
//        UDPMessageSender sender = new UDPMessageSender(this.outgoingMessages, this.myPort);
//        //step 2: create servers
//        createServers();
//        //step2.1: wait for servers to get started
//        try {
//            Thread.sleep(3000);
//        }
//        catch (Exception e) {
//        }
//        printLeaders();
//        //step 3: since we know who will win the election, send requests to the leader, this.leaderPort
//        for (int i = 0; i < this.ports.length; i++) {
//            String invalidClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public int run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
//            sendMessage(invalidClass);
//        }
//        Util.startAsDaemon(sender, "Sender thread");
//        this.incomingMessages = new LinkedBlockingQueue<>();
//        UDPMessageReceiver receiver = new UDPMessageReceiver(this.incomingMessages, this.myAddress, this.myPort, null);
//        Util.startAsDaemon(receiver, "Receiver thread");
//        //step 4: validate responses from leader
//        printResponses();
//
//        //step 5: stop servers
//        stopServers();
//    }
//
//    private void printLeaders() {
//        for (ZooKeeperPeerServer server : this.servers) {
//            Vote leader = server.getCurrentLeader();
//            if (leader != null) {
//                System.out.println("Server on port " + server.getUdpPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
//            }
//        }
//    }
//
//    private void stopServers() {
//        for (ZooKeeperPeerServer server : this.servers) {
//            server.shutdown();
//        }
//    }
//
//    private void printResponses() throws Exception {
//        String completeResponse = "";
//        for (int i = 0; i < this.ports.length; i++) {
//            Message msg = this.incomingMessages.take();
//            String response = new String(msg.getMessageContents());
//            completeResponse += "Response #" + i + ":\n" + response + "\n";
//        }
//        System.out.println(completeResponse);
//    }
//
//    private void sendMessage(String code) throws InterruptedException {
//        Message msg = new Message(Message.MessageType.WORK, code.getBytes(), this.myAddress.getHostString(), this.myPort, "localhost", this.leaderPort);
//        this.outgoingMessages.put(msg);
//    }
//
//    private void createServers() {
//        //create IDs and addresses
//        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(8);
//        for (int i = 0; i < this.ports.length; i++) {
//            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", this.ports[i]));
//        }
//        //create servers
//        this.servers = new ArrayList<>(3);
//        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
//            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
//            map.remove(entry.getKey());
//            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 0);
//            this.servers.add(server);
//            new Thread(server, "Server on port " + server.getUdpPort()).start();
//        }
//    }
//}
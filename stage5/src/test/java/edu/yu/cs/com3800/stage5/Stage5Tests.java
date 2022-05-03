package edu.yu.cs.com3800.stage5;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Stage5Tests {

    @BeforeEach
    public void before() {
        System.out.println("\n---------------------------------------------");
    }

    @Test
    public void simpleTest() throws IOException, InterruptedException {
        System.out.println("Starting simpleTest()\n");
        int[] ports = {7010, 7020, 7030, 7040, 7050, 7060, 7070, 7080, 7105};
        int[] clientPorts = {20000, 20010, 20020, 20030, 20040, 20050, 20060, 20070, 20080, 20090};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8100, 613L, gatewayPeerServer);
        gatewayServer.start();
        ClientImpl client = new ClientImpl("localhost", 5000);
        client.sendCompileAndRunRequest(validClass, gatewayServer.hostName, gatewayServer.port);
        System.out.println(client.getResponse().getCode() + "\n" + client.getResponse().getBody());
        gatewayServer.stop();
        stopServers(servers);
    }

    @Test
    public void multiTest() throws InterruptedException, IOException {
        System.out.println("Starting multiTest()\n");
        int[] ports = {8110, 8120, 8130, 8140, 8150, 8160, 8170, 8180, 8205};
        int[] clientPorts = {20100, 20110, 20120, 20130, 20140, 20150, 20160, 20170, 20180, 20190};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8200, 613L, gatewayPeerServer);
        gatewayServer.start();
        ClientImpl client = new ClientImpl("localhost", 9990);
        for(int i = 1; i < 30; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            client.sendCompileAndRunRequest(code, gatewayServer.hostName, gatewayServer.port);
            if(client.getResponse() != null) System.out.println(client.getResponse().getCode() + "\n" + client.getResponse().getBody());
            else System.out.println("Response is null");
        }
        gatewayServer.stop();
        stopServers(servers);
    }

    @Test
    public void simpleKillServer() throws InterruptedException, IOException {
        System.out.println("Starting simpleKillServer()\n");
        int[] ports = {8210, 8220, 8230, 8240, 8250, 8260, 8270, 8280, 8305};
        int[] clientPorts = {20100, 20110, 20120, 20130, 20140, 20150, 20160, 20170, 20180, 20190};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8300, 613L, gatewayPeerServer);
        gatewayServer.start();
        ClientImpl client = new ClientImpl("localhost", 1990);
        for(int i = 1; i < 30; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            client.sendCompileAndRunRequest(code, gatewayServer.hostName, gatewayServer.port);
            if(i == 5) {
                servers.get(1).shutdown();
                Thread.sleep(35000);
            }
            if(client.getResponse() != null) System.out.println(client.getResponse().getCode() + "\n" + client.getResponse().getBody());
            else System.out.println("Response is null");
        }
        gatewayServer.stop();
        stopServers(servers);
    }

    @Test
    public void simpleKillLeader() throws InterruptedException, IOException {
        System.out.println("Starting simpleKillLeader()\n");
        int[] ports = {8310, 8320, 8330, 8340, 8350, 8360, 8370, 8380, 8405};
        int[] clientPorts = {20100, 20110, 20120, 20130, 20140, 20150, 20160, 20170, 20180, 20190};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8400, 613L, gatewayPeerServer);
        gatewayServer.start();
        ClientImpl client = new ClientImpl("localhost", 1991);
        for(int i = 1; i < 30; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            client.sendCompileAndRunRequest(code, gatewayServer.hostName, gatewayServer.port);
            if(i == 10) {
                servers.get(7).shutdown();
                Thread.sleep(35000);
            }
            if(client.getResponse() != null) System.out.println(client.getResponse().getCode() + "\n" + client.getResponse().getBody());
            else System.out.println("Response is null");
        }
        gatewayServer.stop();
        stopServers(servers);
    }

    @Test
    public void multiKillLeader() throws InterruptedException, IOException {
        System.out.println("Starting multiKillLeader()\n");
        int[] ports = {8410, 8420, 8430, 8440, 8450, 8460, 8470, 8480, 8505};
        int[] clientPorts = {20200, 20210, 20220, 20230, 20240, 20250, 20260, 20270, 20280, 20290};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8500, 613L, gatewayPeerServer);
        gatewayServer.start();

        SimpleClientThread[] clientThreads = new SimpleClientThread[10];

        for(int i = 0; i < 9; i++) {
            clientThreads[i] = new SimpleClientThread(clientPorts[i], gatewayServer);
            clientThreads[i].start();
        }
        while(SimpleClientThread.activeThreads > 0) Thread.sleep(5000);
        servers.get(7).shutdown();
        Thread.sleep(1000);
        clientThreads[9] = new SimpleClientThread(clientPorts[9], gatewayServer);
        clientThreads[9].start();
        while(SimpleClientThread.activeThreads > 0) Thread.sleep(5000);
        gatewayServer.stop();
        stopServers(servers);
    }

    @Test
    public void multiKillFollower() throws InterruptedException, IOException {
        System.out.println("Starting multiKillFollower()\n");
        int[] ports = {8510, 8520, 8530, 8540, 8550, 8560, 8570, 8580, 8605};
        int[] clientPorts = {20300, 20310, 20320, 20330, 20340, 20350, 20360, 20370, 20380, 20390};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8600, 613L, gatewayPeerServer);
        gatewayServer.start();

        SimpleClientThread[] clientThreads = new SimpleClientThread[10];

        for(int i = 0; i < 9; i++) {
            clientThreads[i] = new SimpleClientThread(clientPorts[i], gatewayServer);
            clientThreads[i].start();
        }
        while(SimpleClientThread.activeThreads > 0) Thread.sleep(5000);
        servers.get(3).shutdown();
        Thread.sleep(1000);
        clientThreads[9] = new SimpleClientThread(clientPorts[9], gatewayServer);
        clientThreads[9].start();
        while(SimpleClientThread.activeThreads > 0) Thread.sleep(5000);
        gatewayServer.stop();
        stopServers(servers);
    }

    @Test
    public void multiClientTest() throws InterruptedException, IOException {
        System.out.println("Starting multiClientTest()\n");
        int[] ports = {8610, 8620, 8630, 8640, 8650, 8660, 8670, 8680, 8705};
        int[] clientPorts = {20400, 20410, 20420, 20430, 20440, 20450, 20460, 20470, 20480, 20490};
        ArrayList<ZooKeeperPeerServerImpl> servers = new ArrayList<>();
        GatewayPeerServerImpl gatewayPeerServer = null;
        gatewayPeerServer = createServers(ports, servers, null);
        Thread.sleep(3000);
        GatewayServer gatewayServer = new GatewayServer("localhost", 8700, 613L, gatewayPeerServer);
        gatewayServer.start();

        ClientThread[] clientThreads = new ClientThread[10];

        for(int i = 0; i < 10; i++) {
            clientThreads[i] = new ClientThread(clientPorts[i], gatewayServer);
            clientThreads[i].start();
        }

        while(ClientThread.activeThreads > 0) Thread.sleep(5000);

        gatewayServer.stop();
        stopServers(servers);
    }

    private GatewayPeerServerImpl createServers(int[] ports, ArrayList<ZooKeeperPeerServerImpl> servers, GatewayPeerServerImpl gatewayPeerServer) {
        //create IDs and addresses
        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(9);
        for (int i = 0; i < ports.length; i++) {
            peerIDtoAddress.put(Integer.valueOf(i).longValue(), new InetSocketAddress("localhost", ports[i]));
        }
        //for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
        Iterator<Map.Entry<Long, InetSocketAddress>> iterator = peerIDtoAddress.entrySet().iterator();
        for(int i = 0; i < peerIDtoAddress.entrySet().size(); i++) {
            Map.Entry<Long, InetSocketAddress> entry = iterator.next();
            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
            map.remove(entry.getKey());
            ZooKeeperPeerServerImpl server;
            if(i < peerIDtoAddress.entrySet().size() - 1) {
                server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 1);
            }
            else {
                server = new GatewayPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 1);
                gatewayPeerServer = (GatewayPeerServerImpl) server;
            }
            servers.add(server);
        }
        for(ZooKeeperPeerServerImpl server : servers) {
            //new Thread((Runnable) server, "Server on port " + server.getUdpPort()).start();
            server.start();
        }
        return gatewayPeerServer;
    }

    private void stopServers(ArrayList<ZooKeeperPeerServerImpl> servers) {
        for(ZooKeeperPeerServerImpl server : servers) {
            server.shutdown();
        }
    }
}


class ClientThread extends Thread {
    int port;
    GatewayServer gatewayServer;

    static int activeThreads = 0;

    public ClientThread(int port, GatewayServer gatewayServer) {
        this.port = port;
        this.gatewayServer = gatewayServer;
        activeThreads++;
    }

    @Override
    public void run() {
        ClientImpl client = null;
        try {
            client = new ClientImpl("localhost", this.port);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        for(int i = this.port % 100; i < this.port % 100 + 10; i++) {
            String code = validClass.replace("world!", "world! from code version " + i);
            try {
                assert client != null;
                client.sendCompileAndRunRequest(code, gatewayServer.hostName, gatewayServer.port);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                if(client.getResponse() != null) {
                    System.out.println(client.getResponse().getCode() + "\n" + client.getResponse().getBody());
                    assert client.getResponse().getCode() == 200;
                    if(!client.getResponse().getBody().equals("Hello world! from code version " + i)) {
                        System.err.println("Expected: Hello world! from code version" + i
                                            + "\nReceived: " + client.getResponse().getBody());
                    }
                }
                else System.out.println("Response is null");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        activeThreads--;
    }
}

class SimpleClientThread extends Thread {
    int port;
    GatewayServer gatewayServer;

    static int activeThreads = 0;
    volatile static int i = 0;

    public SimpleClientThread(int port, GatewayServer gatewayServer) {
        this.port = port;
        this.gatewayServer = gatewayServer;
        activeThreads++;
    }

    @Override
    public void run() {
        ClientImpl client = null;
        try {
            client = new ClientImpl("localhost", this.port);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }
        String validClass = "package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world!\";\n    }\n}\n";
        int counter = getI();
        String code = validClass.replace("world!", "world! from code version " + counter);
        try {
            assert client != null;
            client.sendCompileAndRunRequest(code, gatewayServer.hostName, gatewayServer.port);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            if(client.getResponse() != null) {
                System.out.println(client.getResponse().getCode() + "\n" + client.getResponse().getBody());
                assert client.getResponse().getCode() == 200;
                if(!client.getResponse().getBody().equals("Hello world! from code version " + counter)) {
                    System.err.println("Expected: Hello world! from code version" + counter
                            + "\nReceived: " + client.getResponse().getBody());
                }
            }
            else System.out.println("Response is null");
        } catch (IOException e) {
            e.printStackTrace();
        }
        activeThreads--;
    }

    private static synchronized int getI() {
        return ++i;
    }
}
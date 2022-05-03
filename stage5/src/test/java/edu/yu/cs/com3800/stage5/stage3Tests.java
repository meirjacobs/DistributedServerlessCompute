//package edu.yu.cs.com3800.stage4;
//
//import static org.junit.jupiter.api.Assertions.assertEquals;
//import static org.junit.jupiter.api.Assertions.assertTrue;
//
//import edu.yu.cs.com3800.*;
//import org.junit.jupiter.api.*;
//
//import java.io.IOException;
//import java.net.InetSocketAddress;
//import java.net.MalformedURLException;
//import java.net.URI;
//import java.net.http.HttpClient;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Map;
//
//public class stage3Tests {
//    static SimpleServerImpl server;
//
//    @BeforeAll
//    static void starter() throws IOException {
//        server = new SimpleServerImpl(9000);
//        server.start();
//        System.out.println("NOTE: The log files are under the root directory");
//    }
//
//    @BeforeEach
//    public void lineSeparator() {
//        System.out.println("----------------------------------------------------------------------------");
//    }
//
//    @AfterAll
//    public static void ender() {
//        server.stop();
//    }
//
//    @Test
//    public void simpleTest() throws IOException {
//        System.out.println("\nBegin Simple Test\n");
//        String src = "public class DummyClass {public String run() {return \"It's Working!!!\";}}";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "200\nIt's Working!!!";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void runMethodReturnsInt() throws IOException {
//        System.out.println("\nBegin runMethodReturnsInt Test\n");
//        String src = "public class DummyClass {public int run() {return 613;}}";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "400\nThe return type of the class was int, not java.lang.String";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void noRunMethod() throws IOException {
//        System.out.println("\nBegin noRunMethod Test\n");
//        String src = "public class DummyClass {public int walk() {return 613;}}";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "400\nCould not create and run instance of class";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void argsConstructor() throws IOException {
//        System.out.println("\nBegin argsConstructor Test\n");
//        String src = "public class DummyClass {public DummyClass(int number){number++;} public int walk() {return 613;}}";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "400\nCould not create and run instance of class";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void runMethodTakesArgument() throws IOException {
//        System.out.println("\nBegin runMethodTakesArgument Test\n");
//        String src = "public class DummyClass {public String run(String s) {return s;}}";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "400\nCould not create and run instance of class";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void javaCodeShouldntCompile() throws IOException {
//        System.out.println("\nBegin javaCodeShouldntCompile Test\n");
//        String src = "public class DummyClass {\n\npublic String run() {\nreturn 613;\n}\n}\n";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "400\nCode did not compile:\nError on line 4, column 8 in string:///DummyClass.java\n";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void newlineCharacters() throws IOException {
//        System.out.println("\nBegin newlineCharacters Test\n");
//        String src = "public class DummyClass {\n\npublic String run() {\nreturn \"It's Working!!!\";\n}\n}\n";
//        ClientImpl client = new ClientImpl("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "200\nIt's Working!!!";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void badClient() throws IOException {
//        System.out.println("\nBegin BadClient Test\n");
//        String src = "public class DummyClass {\n\npublic String run() {\nreturn \"It's Working!!!\";\n}\n}\n";
//        BadClient client = new BadClient("http://localhost:", 9000);
//        client.sendCompileAndRunRequest(src);
//        Client.Response response = client.getResponse();
//        String expectedResponse = "400\n";
//        String actualResponse = response.getCode() + "\n" + response.getBody();
//
//        System.out.println("Expected response:");
//        System.out.println(expectedResponse);
//        System.out.println("\nActual response:");
//        System.out.println(actualResponse);
//
//        assertTrue(actualResponse.startsWith(expectedResponse.trim()));
//        //assertEquals(expectedResponse.trim(), actualResponse.trim());
//    }
//
//    @Test
//    public void manyServers() {
//        //create IDs and addresses
//        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
//        for(int i = 1, p = 9001; i < 30; i++, p++) {
//            peerIDtoAddress.put((long)i, new InetSocketAddress("localhost", p));
//        }
//
//        //create servers
//        ArrayList<ZooKeeperPeerServer> servers = new ArrayList<>(3);
//        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
//            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
//            map.remove(entry.getKey());
//            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 0);
//            servers.add(server);
//            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
//        }
//        //wait for threads to start
//        try {
//            Thread.sleep(5000);
//        }
//        catch (Exception e) {
//        }
//        //print out the leaders and shutdown
//        for (ZooKeeperPeerServer server : servers) {
//            Vote leader = server.getCurrentLeader();
//            if (leader != null) {
//                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
//                server.shutdown();
//            }
//        }
//    }
//
//    @Test
//    public void shutdowns() {
//        //create IDs and addresses
//        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
//        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
//        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
//        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
//        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
//        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
//        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
//        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
//        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
//
//        //create servers
//        ArrayList<ZooKeeperPeerServer> servers = new ArrayList<>(3);
//        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
//            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
//            map.remove(entry.getKey());
//            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 0);
//            servers.add(server);
//            if(server.getServerId() == 8) {
//                server.shutdown();
//            }
//            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
//        }
//        //wait for threads to start
//        try {
//            Thread.sleep(500);
//        }
//        catch (Exception e) {
//        }
//        //print out the leaders and shutdown
//        //boolean flag = false;
//        long leaderID = servers.get(0).getCurrentLeader().getProposedLeaderID();
//        for (ZooKeeperPeerServer server : servers) {
//            Vote leader = server.getCurrentLeader();
//            if (leader != null) {
//                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
//                //assertTrue((server.getCurrentLeader().getProposedLeaderID() != 8 && server.getCurrentLeader().getProposedLeaderID() == leaderID) || server.getServerId() == 8);
//                server.shutdown();
//            }
//        }
//    }
//
//    @Test
//    public void delayed8() {
//        //create IDs and addresses
//        HashMap<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>(3);
//        peerIDtoAddress.put(1L, new InetSocketAddress("localhost", 8010));
//        peerIDtoAddress.put(2L, new InetSocketAddress("localhost", 8020));
//        peerIDtoAddress.put(3L, new InetSocketAddress("localhost", 8030));
//        peerIDtoAddress.put(4L, new InetSocketAddress("localhost", 8040));
//        peerIDtoAddress.put(5L, new InetSocketAddress("localhost", 8050));
//        peerIDtoAddress.put(6L, new InetSocketAddress("localhost", 8060));
//        peerIDtoAddress.put(7L, new InetSocketAddress("localhost", 8070));
//        peerIDtoAddress.put(8L, new InetSocketAddress("localhost", 8080));
//
//        //create servers
//        ArrayList<ZooKeeperPeerServer> servers = new ArrayList<>(3);
//        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
//            HashMap<Long, InetSocketAddress> map = (HashMap<Long, InetSocketAddress>) peerIDtoAddress.clone();
//            map.remove(entry.getKey());
//            ZooKeeperPeerServerImpl server = new ZooKeeperPeerServerImpl(entry.getValue().getPort(), 0, entry.getKey(), map, 0);
//            servers.add(server);
//            new Thread(server, "Server on port " + server.getAddress().getPort()).start();
//            if(server.getCurrentLeader().getProposedLeaderID() == 7) {
//                try {
//                    Thread.sleep(400);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
//        }
//
//        //wait for threads to start
//        try {
//            Thread.sleep(500);
//        }
//        catch (Exception e) {
//        }
//
//        //print out the leaders and shutdown
//        //boolean flag = false;
//        long leaderID = servers.get(0).getCurrentLeader().getProposedLeaderID();
//        for (ZooKeeperPeerServer server : servers) {
//            Vote leader = server.getCurrentLeader();
//            if (leader != null) {
//                System.out.println("Server on port " + server.getAddress().getPort() + " whose ID is " + server.getServerId() + " has the following ID as its leader: " + leader.getProposedLeaderID() + " and its state is " + server.getPeerState().name());
//                //assertTrue((server.getCurrentLeader().getProposedLeaderID() != 8 && server.getCurrentLeader().getProposedLeaderID() == leaderID) || server.getServerId() == 8);
//                server.shutdown();
//            }
//        }
//    }
//
//    /*@Test
//    public void purposelyFail() {
//        assertEquals(true, false);
//    }*/
//
//
//}
//
//class BadClient implements Client{
//    String hostName;
//    int hostPort;
//    Response response;
//    private final HttpClient httpClient = HttpClient.newBuilder()
//            .version(HttpClient.Version.HTTP_2)
//            .build();
//
//    public BadClient(String hostName, int hostPort) throws MalformedURLException {
//        this.hostName = hostName;
//        this.hostPort = hostPort;
//    }
//
//    public void sendCompileAndRunRequest(String src) throws IOException {
//        HttpRequest request = HttpRequest.newBuilder()
//                .POST(HttpRequest.BodyPublishers.ofString(src))
//                .uri(URI.create(this.hostName + this.hostPort + "/compileandrun"))
//                .setHeader("Content-Type", "NOT-text/x-java-source")
//                .version(HttpClient.Version.HTTP_2)
//                .build();
//
//        try {
//            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
//            this.response = new Response(response.statusCode(), response.body());
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    public Response getResponse() throws IOException {
//        return this.response;
//    }
//}

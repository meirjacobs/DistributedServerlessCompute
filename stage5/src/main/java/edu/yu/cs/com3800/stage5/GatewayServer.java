package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class GatewayServer implements LoggingServer {
    HttpServer httpServer;
    InetSocketAddress leaderAddress;
    GatewayPeerServerImpl gatewayPeerServer;
    String hostName;
    int port;
    long id;
    Logger logger;
    Set<Message> unfinishedMessagesToLeader = new HashSet<>();
    volatile long requestID = 0;

    public static void main(String[] args) {
//        System.out.println("Creating gateway");
        ConcurrentHashMap<Long,InetSocketAddress> map = new ConcurrentHashMap<>();
        int port = 8010;
        for (long i = 1; i <= 7; i++) {
            map.put(i, new InetSocketAddress("localhost", port));
            port += 10;
        }
        GatewayPeerServerImpl gatewayPeerServer = new GatewayPeerServerImpl(port + 5, 0, Long.parseLong(args[0]), map, 1);
        gatewayPeerServer.start();
//        System.out.println("Gateway on port " + port);
        new GatewayServer("localhost", port, 613L, gatewayPeerServer).start();
    }

    public GatewayServer(String hostName, int port, Long id, GatewayPeerServerImpl gatewayPeerServer) {
        this.hostName = hostName;
        this.port = port;
        this.id = id;
        this.gatewayPeerServer = gatewayPeerServer;
        try {
            logger = initializeLogging(Util.getLoggerName(GatewayServer.class.getSimpleName(), id, port));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        while(gatewayPeerServer.getCurrentLeader() == null ||
                gatewayPeerServer.getCurrentLeader().getProposedLeaderID() == gatewayPeerServer.getId()) Thread.onSpinWait();
        this.leaderAddress = gatewayPeerServer.getPeerByID(gatewayPeerServer.getCurrentLeader().getProposedLeaderID());
        this.httpServer = null;
        try {
            httpServer = HttpServer.create(new InetSocketAddress(this.port), 100);
        } catch (IOException e) {
            logger.info(Util.getStackTrace(e));
        }
        assert httpServer != null;
        httpServer.createContext("/compileandrun", new Handler());
        httpServer.createContext("/getNodes", new GetNodesHandler());
        httpServer.createContext("/isDead", new IsDeadHandler());
        httpServer.createContext("/getGossips", new GetGossipsHandler());
        httpServer.setExecutor(Executors.newCachedThreadPool()); // creates a default executor
        httpServer.start();
    }

    public void stop() {
        httpServer.removeContext("/compileandrun");
        httpServer.stop(0);
    }

    private InetSocketAddress getLeaderAddress() {
        logger.info("Current leader is server ID " + gatewayPeerServer.getCurrentLeader().getProposedLeaderID() +
                "\nAnd gatewaypeerserver ID is " + gatewayPeerServer.getServerId());
        while(gatewayPeerServer.getCurrentLeader().getProposedLeaderID() == gatewayPeerServer.getServerId()) Thread.onSpinWait();
        long leaderID = gatewayPeerServer.getCurrentLeader().getProposedLeaderID();
        while(gatewayPeerServer.isPeerDead(leaderID) || gatewayPeerServer.getCurrentLeader().getProposedLeaderID() == gatewayPeerServer.getServerId()) {
            logger.info("Waiting for new leader to be elected");
            leaderID = leaderFailedProtocol(leaderID);
            logger.info("New leader elected");
        }
        return gatewayPeerServer.getPeerByID(leaderID);
    }

    private long leaderFailedProtocol(long leaderID) {
        while(gatewayPeerServer.isPeerDead(leaderID) || gatewayPeerServer.getServerId() == leaderID) {
            leaderID = gatewayPeerServer.getCurrentLeader().getProposedLeaderID();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
//        System.out.println(leaderID + ", " + gatewayPeerServer.getServerId());
        logger.info("Gatewaypeerserver ID is " + gatewayPeerServer.getServerId());
        logger.info("New leader ID is " + leaderID);
        // TODO: Send unfinishedMessagesToLeader to the new leader
        return leaderID;
    }

    private synchronized long getNewRequestID() {
        return ++requestID;
    }

    private Logger getLogger() {
        return logger;
    }

    private String getHostName() {
        return this.hostName;
    }

    private int getPort() {
        return this.port;
    }

    private void addToUnfinishedLeaderMessages(Message message) {
        unfinishedMessagesToLeader.add(message);
    }

    private void removeFromUnfinishedLeaderMessages(Message message) {
        unfinishedMessagesToLeader.remove(message);
    }

    private boolean handlerResponseLeaderDead(InetSocketAddress leaderAddr) {
        if(gatewayPeerServer.isPeerDead(leaderAddr)) {
            // TODO: Wait for new leader and send unfinished work to it
            return true;
        }
        return false;
    }

    private String getNodeList() {
        return gatewayPeerServer.getNodeIDs();
    }

    private void waitUntilNodeIsDead(long id) {
        gatewayPeerServer.waitUntilNodeIsDead(id);
    }

    private class Handler implements HttpHandler {

        public Handler(){}

        public void handle(HttpExchange exchange) throws IOException {
            // Bad input handling
            Logger logger1 = getLogger();
            logger1.info("Received exchange");
            List<String> type = exchange.getRequestHeaders().get("Content-Type");
            if(type == null || !type.contains("text/x-java-source")) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, 0);
                exchange.getResponseBody().write(new byte[0]);
                logger1.info("HTTP_BAD_REQUEST: 400\nContent-Type is not text/x-java-source");
                exchange.close();
                return;
            }

            // Send task to leader via TCP
            InetSocketAddress leaderAddr = getLeaderAddress();
            InputStream is = exchange.getRequestBody();
            byte[] data = is.readAllBytes();
            Message message = new Message(Message.MessageType.WORK, data, getHostName(), getPort(), leaderAddr.getHostName(), leaderAddr.getPort(), getNewRequestID());
            addToUnfinishedLeaderMessages(message);
            int servPort = leaderAddr.getPort() + 2;
            logger1.info("Handling message with ID " + message.getRequestID());
            Socket leaderSocket = null;
            boolean error = false;
            try {
                leaderSocket = new Socket("localhost", servPort);
            }
            catch (Exception e) {
                logger1.info("ERROR: " + e.getMessage() + "\nCannot connect to leader socket");
                error = true;
            }
            while(error) {
                leaderAddr = getLeaderAddress();
                servPort = leaderAddr.getPort() + 2;
                error = false;
                try {
                    leaderSocket = new Socket("localhost", servPort);
                }
                catch (Exception e) {
                    error = true;
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
            logger1.info("Connected to socket at " + leaderSocket.getInetAddress() + ":" + leaderSocket.getLocalPort());
            OutputStream out = leaderSocket.getOutputStream();
            InputStream in = leaderSocket.getInputStream();
            out.write(message.getNetworkPayload());
            byte[] bytes = Util.readAllBytesFromNetworkWithFailure(in, 10000, logger1);
            if(bytes == null) logger1.info("Bytes[] is null after sending in request ID " + message.getRequestID());
            while(bytes == null) {
                leaderAddr = getLeaderAddress();
                servPort = leaderAddr.getPort() + 2;
                leaderSocket = new Socket("localhost", servPort);
                logger1.info("Connected to socket at " + leaderSocket.getInetAddress() + ":" + leaderSocket.getLocalPort());
                out = leaderSocket.getOutputStream();
                in = leaderSocket.getInputStream();
                out.write(message.getNetworkPayload());
                bytes = Util.readAllBytesFromNetworkWithFailure(in, 10000, logger1);
                if(bytes == null) logger1.info("Bytes[] is null after sending in request ID " + message.getRequestID());
            }
            Message returnMessage = new Message(bytes);
            logger1.info("Received response for message ID " + message.getRequestID());
            leaderSocket.close();
            out.close();
            in.close();

            if(handlerResponseLeaderDead(leaderAddr)) {
                exchange.close();
                logger1.info("Ignored response from dead leader");
                return;
            }
            removeFromUnfinishedLeaderMessages(message);

            // Returning response
            if(!returnMessage.getErrorOccurred()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, returnMessage.getMessageContents().length);
                exchange.getResponseBody().write(returnMessage.getMessageContents());
                logger1.info(HttpURLConnection.HTTP_OK + "\n" + new String(returnMessage.getMessageContents()));
            }
            else {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, returnMessage.getMessageContents().length);
                exchange.getResponseBody().write(returnMessage.getMessageContents());
                logger1.info(HttpURLConnection.HTTP_BAD_REQUEST + "\n" +  new String(returnMessage.getMessageContents()));
            }
            exchange.close();
        }
    }

    private class GetNodesHandler implements HttpHandler {

        public GetNodesHandler(){}

        public void handle(HttpExchange exchange) throws IOException {
//            System.out.println("Received GetNodes request");
            Logger logger1 = getLogger();
            logger1.info("Received GetNodesHandler exchange");

            String nodeList = getNodeList();
//            System.out.println("got nodes");
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, nodeList.getBytes().length);
            exchange.getResponseBody().write(nodeList.getBytes());
            logger1.info("Responded to request with node list: " + nodeList);
            exchange.close();
        }
    }

    private class IsDeadHandler implements HttpHandler {

        public IsDeadHandler(){}

        public void handle(HttpExchange exchange) throws IOException {
            Logger logger1 = getLogger();
            logger1.info("Received IsDeadHandler exchange");

            long id = Long.parseLong(new String(exchange.getRequestBody().readAllBytes()));
            waitUntilNodeIsDead(id);

            String responseBody = "OK";
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.getBytes().length);
            exchange.getResponseBody().write(responseBody.getBytes());
            logger1.info("Responded to IsDead request");
            exchange.close();
        }
    }

    private class GetGossipsHandler implements HttpHandler {

        public GetGossipsHandler(){}

        public void handle(HttpExchange exchange) throws IOException {
            Logger logger1 = getLogger();
            logger1.info("Received GetGossipsHandler exchange");

            if(gatewayPeerServer == null) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_NO_CONTENT, 0);
                logger1.info("Responded to GetGossipsHandler request");
                exchange.close();
                return;
            }

            List<HeartbeatMonitor.GossipMessageInfo> list = gatewayPeerServer.heartbeatMonitor.getListOfReceivedGossips();
            StringBuilder stringBuilder = new StringBuilder();
            for(HeartbeatMonitor.GossipMessageInfo gossipMessageInfo : list) {
                stringBuilder.append(gossipMessageInfo.toString());
            }

            String responseBody = stringBuilder.toString();
            exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, responseBody.getBytes().length);
            exchange.getResponseBody().write(responseBody.getBytes());
            logger1.info("Responded to GetGossipsHandler request");
            exchange.close();
        }
    }
}

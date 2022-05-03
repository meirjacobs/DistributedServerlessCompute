package edu.yu.cs.com3800.stage4;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
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
//        if(!gatewayPeerServer.isAlive()) {
//            gatewayPeerServer.getLogger().info("AHA GOTEM");
//            gatewayPeerServer.start();
//        }
        while(gatewayPeerServer.getCurrentLeader().getProposedLeaderID() == gatewayPeerServer.getId()) Thread.onSpinWait();
        this.leaderAddress = gatewayPeerServer.getPeerByID(gatewayPeerServer.getCurrentLeader().getProposedLeaderID());
        this.httpServer = null;
        try {
            httpServer = HttpServer.create(new InetSocketAddress(this.port), 100);
        } catch (IOException e) {
            logger.info(Util.getStackTrace(e));
        }
        assert httpServer != null;
        httpServer.createContext("/compileandrun", new Handler());
        httpServer.setExecutor(Executors.newCachedThreadPool()); // creates a default executor
        httpServer.start();
    }

    public void stop() {
        httpServer.removeContext("/compileandrun");
        httpServer.stop(0);
    }

    private InetSocketAddress getLeaderAddress() {
        while(gatewayPeerServer.getCurrentLeader().getProposedLeaderID() == gatewayPeerServer.getId()) Thread.onSpinWait();
        assert gatewayPeerServer.getCurrentLeader().getProposedLeaderID() != 8090;
        return gatewayPeerServer.getPeerByID(gatewayPeerServer.getCurrentLeader().getProposedLeaderID());
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
                // todo: see if using logger violates the constraint of not having instance variables in the Handler
                logger1.info("HTTP_BAD_REQUEST: 400\nContent-Type is not text/x-java-source");
                exchange.close();
                return;
            }

            // Send task to leader via TCP
            InetSocketAddress leaderAddr = getLeaderAddress();
//            System.out.println("here1");
            InputStream is = exchange.getRequestBody();
//            System.out.println("here1.5");
//            byte[] data = Util.readAllBytesFromNetwork(is);
            byte[] data = is.readAllBytes();
//            System.out.println("here2");
            Message message = new Message(Message.MessageType.WORK, data, getHostName(), getPort(), leaderAddr.getHostName(), leaderAddr.getPort());
//            System.out.println("here3");
            int servPort = leaderAddr.getPort() + 2;
//            logger1.info("Got here");
            Socket leaderSocket = new Socket("localhost", servPort);
            logger1.info("Connected to socket at " + leaderSocket.getInetAddress() + ":" + leaderSocket.getLocalPort());
            OutputStream out = leaderSocket.getOutputStream();
            InputStream in = leaderSocket.getInputStream();
            out.write(message.getNetworkPayload());

            message = new Message(Util.readAllBytesFromNetwork(in));
            leaderSocket.close();
            out.close();
            in.close();

            // Returning response
            if(!message.getErrorOccurred()) {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, message.getMessageContents().length);
                exchange.getResponseBody().write(message.getMessageContents());
                logger1.info(HttpURLConnection.HTTP_OK + "\n" + Arrays.toString(message.getMessageContents()));
            }
            else {
                exchange.sendResponseHeaders(HttpURLConnection.HTTP_BAD_REQUEST, message.getMessageContents().length);
                exchange.getResponseBody().write(message.getMessageContents());
                logger1.info(HttpURLConnection.HTTP_BAD_REQUEST + "\n" + Arrays.toString(message.getMessageContents()));
            }
            exchange.close();
        }
    }
}

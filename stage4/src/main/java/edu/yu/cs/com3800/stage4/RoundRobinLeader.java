package edu.yu.cs.com3800.stage4;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    ZooKeeperPeerServerImpl server;
    LinkedBlockingQueue<Message> incomingMessages;
    List<Long> workerServerIDs;
    boolean shutdown = false;
    Executor executor;
    InetSocketAddress gatewayAddress;
    Logger logger;
    TCPServerThread tcpServerThread;
    Map<Long, OutputStream> requestIDToSocketOutputStream = new HashMap<>();

    private static volatile long requestID = 0;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, List<Long> workerServerIDs, Logger logger) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.workerServerIDs = workerServerIDs;
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.logger = logger;
        logger.info("RoundRobinLeader created");
        this.tcpServerThread = new TCPServerThread(server.myPort + 2, logger, incomingMessages, this);
        Util.startAsDaemon(this.tcpServerThread, "TCPServerThread");
    }

    @Override
    public void run() {
        this.logger.info("RoundRobinLeader started");
        Message m;
        int counter = 0;
        while(!this.isInterrupted() && server.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING && !shutdown) {
            if((m = incomingMessages.poll()) == null) continue;
            // If work message
            if(m.getMessageType() == Message.MessageType.WORK) {
                InetSocketAddress target = server.getPeerByID(workerServerIDs.get(counter));
                logger.info("Target = " + target + ", Counter = " + counter);
                while(target.equals(this.server.myAddress) || isGatewayServerImpl(target, gatewayAddress)) {
                    counter = incrementCounter(counter, workerServerIDs.size() - 1);
                    target = server.getPeerByID(workerServerIDs.get(counter));
                    logger.info("New Target = " + target + ", New Counter = " + counter);
                }
                Message newWorkMessage = new Message(Message.MessageType.WORK, m.getMessageContents(), m.getSenderHost(), m.getSenderPort(), target.getHostName(), target.getPort(), m.getRequestID());
                executor.execute(new CommWithWorkerOverTCP(target, newWorkMessage, gatewayAddress, logger, this));
                counter = incrementCounter(counter, workerServerIDs.size() - 1);
            }
            // If some other message just put back in queue and will be dealt with in a later stage
            else {
                incomingMessages.offer(m);
            }
        }
    }

    private static boolean isGatewayServerImpl(InetSocketAddress target, InetSocketAddress gatewayServerAddress) {
        return gatewayServerAddress.getHostName().equals(target.getHostName())
                && target.getPort() == gatewayServerAddress.getPort() + 5;
    }

    private void setGatewayAddress(InetSocketAddress gatewayAddress) {
        this.gatewayAddress = gatewayAddress;
    }

    private static int incrementCounter(int counter, int maxCounter) {
        return counter < maxCounter ? ++counter : 0;
    }

    public void shutdown() {
        this.shutdown = true;
        logger.info("Server shutdown");
        this.tcpServerThread.interrupt();
        interrupt();
    }

    private synchronized OutputStream getOutputStream(long requestID) {
        return requestIDToSocketOutputStream.get(requestID);
    }

    private synchronized void setOutputStream(long requestID, OutputStream outputStream) {
        if(outputStream == null) {
            requestIDToSocketOutputStream.remove(requestID);
        }
        else {
            requestIDToSocketOutputStream.put(requestID, outputStream);
        }
    }

    private synchronized long getNewRequestID() {
        return ++requestID;
    }

    private static class TCPServerThread extends Thread {
        int port;
        Logger logger;
        LinkedBlockingQueue<Message> incomingMessages;
        RoundRobinLeader roundRobinLeader;

        public TCPServerThread(int port, Logger logger, LinkedBlockingQueue<Message> incomingMessages, RoundRobinLeader roundRobinLeader) {
            this.port = port;
            this.logger = logger;
            this.incomingMessages = incomingMessages;
            this.roundRobinLeader = roundRobinLeader;
            this.logger.info("TCPServerThread created");
        }

        @Override
        public void run() {
            this.logger.info("TCPServerThread started");
            /* Accept TCP connections and forwards requests to the RoundRobinServer */
            try {
                // Create a server socket to accept client connection requests
                ServerSocket servSock = new ServerSocket(port);

                while (!this.isInterrupted()) { // Run forever, accepting and servicing connections
                    Socket clntSock = servSock.accept();     // Get client connection
                    logger.info("Handling client");
                    InputStream in = clntSock.getInputStream();
                    OutputStream out = clntSock.getOutputStream();
                    Message message = new Message(Util.readAllBytesFromNetwork(in));
                    Message realMessage = new Message(message.getMessageType(), message.getMessageContents(), message.getSenderHost(), message.getSenderPort(), message.getReceiverHost(), message.getReceiverPort(), this.roundRobinLeader.getNewRequestID());
                    this.roundRobinLeader.setGatewayAddress(new InetSocketAddress(realMessage.getSenderHost(), realMessage.getSenderPort()));
                    this.roundRobinLeader.setOutputStream(realMessage.getRequestID(), out);
                    incomingMessages.offer(realMessage);
                    logger.info("Put message " + realMessage + " onto queue for RoundRobinLeader to process");
                }
            } catch (Exception e) {
                logger.severe(Util.getStackTrace(e));
            }
        }
    }

    private static class CommWithWorkerOverTCP implements Runnable {
        InetSocketAddress workerAddress;
        Message message;
        InetSocketAddress gatewayAddress;
        Logger logger;
        RoundRobinLeader roundRobinLeader;

        public CommWithWorkerOverTCP(InetSocketAddress workerAddress, Message message, InetSocketAddress gatewayAddress, Logger logger, RoundRobinLeader roundRobinLeader) {
            this.workerAddress = workerAddress;
            this.message = message;
            this.gatewayAddress = gatewayAddress;
            this.logger = logger;
            this.roundRobinLeader = roundRobinLeader;
            this.logger.info("CommWithWorkerThread created");
        }

        @Override
        public void run() {
            this.logger.info("CommWithWorkerThread started");
            Socket workerSocket = null;
            try {
                workerSocket = new Socket(workerAddress.getHostName(), workerAddress.getPort());
                this.logger.info("Connected to worker socket");
                OutputStream outToWorker = workerSocket.getOutputStream();
                InputStream inFromWorker = workerSocket.getInputStream();
                outToWorker.write(message.getNetworkPayload());
                this.logger.info("Wrote to worker");

                OutputStream os = this.roundRobinLeader.getOutputStream(message.getRequestID());
                if(os == null) throw new Exception("Output stream of socket connection for requestID " + message.getRequestID() + " isn't in map");
                os.write(Util.readAllBytesFromNetwork(inFromWorker));
                logger.info("Responded to gateway server");
                this.roundRobinLeader.setOutputStream(message.getRequestID(), null);
            }
            catch (Exception e) {
                logger.severe(Util.getStackTrace(e));
            }
            finally {
                try {
                    if(workerSocket != null) workerSocket.close();
                    else logger.warning("Worker socket is null, can't close it");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

}

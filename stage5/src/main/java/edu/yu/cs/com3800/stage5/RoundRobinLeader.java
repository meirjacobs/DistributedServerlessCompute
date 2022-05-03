package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.ZooKeeperPeerServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    ZooKeeperPeerServerImpl server;
    LinkedBlockingQueue<Message> incomingMessages;
    volatile List<Long> workerServerIDs;
    volatile boolean shutdown = false;
    Executor executor;
    volatile InetSocketAddress gatewayAddress;
    Logger logger;
    TCPServerThread tcpServerThread;
    Map<Long, OutputStream> requestIDToSocketOutputStream = new HashMap<>();
    Map<InetSocketAddress, Queue<Message>> idToMessages = new HashMap<>();
    Queue<Message> queuedUpWork = new LinkedList<>();
    Map<Long, Message> queuedUpWorkResponses = new HashMap<>();

    //private static volatile long requestID = 0;

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, List<Long> workerServerIDs, Queue<Message> queuedUpWork, Logger logger) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.workerServerIDs = workerServerIDs;
        this.executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        this.queuedUpWork.addAll(queuedUpWork);
        this.logger = logger;
        logger.info("RoundRobinLeader created");
        this.tcpServerThread = new TCPServerThread(server.myPort + 2, logger, incomingMessages, this);
        Util.startAsDaemon(this.tcpServerThread, "TCPServerThread");
    }

    public RoundRobinLeader(ZooKeeperPeerServerImpl server, LinkedBlockingQueue<Message> incomingMessages, List<Long> workerServerIDs, Queue<Message> queuedUpWork, InetSocketAddress gatewayAddress, Logger logger) {
        this(server, incomingMessages, workerServerIDs, queuedUpWork, logger);
        this.gatewayAddress = gatewayAddress;
    }

    @Override
    public void run() {
        this.logger.info("RoundRobinLeader started");
        requestAllQueuedUpCompletedWork();
//        while(gatewayAddress == null) Thread.onSpinWait();
//        logger.info("Gateway address established");
        Message m;
        int counter = 0;
        while(!this.isInterrupted() && server.getPeerState() == ZooKeeperPeerServer.ServerState.LEADING && !shutdown) {
            if((m = incomingMessages.poll()) == null) continue;
            logger.info("polling message");
            // If work message
            if(m.getMessageType() == Message.MessageType.WORK) {
                if(queuedUpWorkResponses.containsKey(m.getRequestID())) {
                    OutputStream out = requestIDToSocketOutputStream.get(m.getRequestID());
                    try {
                        out.write(queuedUpWorkResponses.get(m.getRequestID()).getNetworkPayload());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    setOutputStream(m.getRequestID(), null);
                    queuedUpWorkResponses.remove(m.getRequestID());
                    continue;
                }
                InetSocketAddress target = server.getPeerByID(workerServerIDs.get(counter));
                logger.info("Target = " + target + ", Counter = " + counter);
                if(server.isPeerDead(target) && idToMessages.get(target) != null) {
                    logger.warning(target.toString() + " is dead. Redistributing its work to other workers.");
                    //workerServerIDs.remove(counter);
                    counter = incrementCounter(counter, workerServerIDs.size() - 1);
                    for(Message message : idToMessages.get(target)) {
                        InetSocketAddress newTarget = server.getPeerByID(workerServerIDs.get(counter));
                        while(server.isPeerDead(newTarget)) {
                            counter = incrementCounter(counter, workerServerIDs.size() - 1);
                            newTarget = server.getPeerByID(workerServerIDs.get(counter));
                        }
                        Queue<Message> queue = idToMessages.getOrDefault(newTarget, new LinkedList<>());
                        queue.add(message);
                        idToMessages.put(newTarget, queue);
                        logger.info("Redistributing work with request ID " + message.getRequestID() + " to server " + target.toString());
                        executor.execute(new CommWithWorkerOverTCP(newTarget, message, queue, gatewayAddress, logger, this));
                        counter = incrementCounter(counter, workerServerIDs.size() - 1);
                    }
                    idToMessages.remove(target);
                    target = server.getPeerByID(workerServerIDs.get(counter));
                }
                while(target.equals(this.server.myAddress) || isGatewayServerImpl(target, gatewayAddress) || server.isPeerDead(target)) {
                    // TODO: Clarify that .size() will be accurate even though a failed server could be deleted in the interim
                    counter = incrementCounter(counter, workerServerIDs.size() - 1);
                    target = server.getPeerByID(workerServerIDs.get(counter));
                    logger.info("New Target = " + target + ", New Counter = " + counter);
                }
                Message newWorkMessage = new Message(Message.MessageType.WORK, m.getMessageContents(), m.getSenderHost(), m.getSenderPort(), target.getHostName(), target.getPort(), m.getRequestID());
                Queue<Message> queue = idToMessages.getOrDefault(target, new LinkedList<>());
                queue.add(newWorkMessage);
                idToMessages.put(target, queue);
                executor.execute(new CommWithWorkerOverTCP(target, newWorkMessage, queue, gatewayAddress, logger, this));
                counter = incrementCounter(counter, workerServerIDs.size() - 1);
            }
            // If some other message just put back in queue and will be dealt with in a later stage
            else {
                incomingMessages.offer(m);
            }
        }
    }

    private void requestAllQueuedUpCompletedWork() {
        for (Long workerServerID : workerServerIDs) {
            // TODO: Clarify that .size() will be accurate even though a failed server could be deleted in the interim
            InetSocketAddress target = server.getPeerByID(workerServerID);
            if (target.equals(this.server.myAddress) || isGatewayServerImpl(target, gatewayAddress) || server.isPeerDead(target)) {
                continue;
            }
            byte[] empty = new byte[0];
            Message newWorkMessage = new Message(Message.MessageType.NEW_LEADER_GETTING_LAST_WORK, empty, server.myAddress.getHostName(), server.myPort, target.getHostName(), target.getPort(), -1);
            Queue<Message> queue = idToMessages.getOrDefault(target, new LinkedList<>());
            queue.add(newWorkMessage);
            idToMessages.put(target, queue);
            logger.info("Sending request for completed work to client " + target);
            executor.execute(new CommWithWorkerOverTCP(target, newWorkMessage, queue, gatewayAddress, logger, this));
        }
    }

    private static boolean isGatewayServerImpl(InetSocketAddress target, InetSocketAddress gatewayServerAddress) {
        if(target == null || gatewayServerAddress == null) return false;
        return gatewayServerAddress.getHostName().equals(target.getHostName())
                && target.getPort() == gatewayServerAddress.getPort() + 5;
    }

    private void setGatewayAddress(InetSocketAddress gatewayAddress) {
        logger.info("Setting gatewayserver address to " + gatewayAddress);
        this.gatewayAddress = gatewayAddress;
        ZooKeeperPeerServerImpl.setGatewayServerAddress(gatewayAddress);
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

//    private synchronized long getNewRequestID() {
//        return ++requestID;
//    }

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
                logger.info("Server socket created");

                while (!this.isInterrupted()) { // Run forever, accepting and servicing connections
                    logger.info("Entered while loop");
                    Socket clntSock = servSock.accept();     // Get client connection
                    logger.info("Handling client");
                    InputStream in = clntSock.getInputStream();
                    OutputStream out = clntSock.getOutputStream();
                    Message realMessage = new Message(Util.readAllBytesFromNetwork(in));
                    //Message realMessage = new Message(message.getMessageType(), message.getMessageContents(), message.getSenderHost(), message.getSenderPort(), message.getReceiverHost(), message.getReceiverPort(), this.roundRobinLeader.getNewRequestID());
                    if(roundRobinLeader.gatewayAddress == null) this.roundRobinLeader.setGatewayAddress(new InetSocketAddress(realMessage.getSenderHost(), realMessage.getSenderPort()));
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
        Queue<Message> queue;
        InetSocketAddress gatewayAddress;
        Logger logger;
        RoundRobinLeader roundRobinLeader;

        public CommWithWorkerOverTCP(InetSocketAddress workerAddress, Message message, Queue<Message> queue,
                                     InetSocketAddress gatewayAddress, Logger logger, RoundRobinLeader roundRobinLeader) {
            this.workerAddress = workerAddress;
            this.message = message;
            this.queue = queue;
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

                if(message.getMessageType() == Message.MessageType.NEW_LEADER_GETTING_LAST_WORK) {
                    logger.info("Waiting for response...");
                    byte[] bytes = Util.readAllBytesFromNetworkWithFailure(inFromWorker, 5000, logger);
                    if(bytes == null) return;
                    logger.info("Receiving response...");
                    if(bytes.length == 0) return;
                    Message returned = new Message(bytes);
                    logger.info("Received NEW_LEADER_GETTING_LAST_WORK response for request ID " + returned.getRequestID());
                    this.roundRobinLeader.queuedUpWorkResponses.put(returned.getRequestID(), returned);
                    return;
                }

                OutputStream os = this.roundRobinLeader.getOutputStream(message.getRequestID());
                if(os == null) throw new Exception("Output stream of socket connection for requestID " + message.getRequestID() + " isn't in map");
                byte[] bytes = Util.readAllBytesFromNetworkWithFailure(inFromWorker, 10000, logger);
                if(bytes == null) {
                    logger.info("Didn't get a response for request ID " + message.getRequestID() + " from server " + workerAddress);
                    return;
                }
                os.write(bytes);
                logger.info("Responded to gateway server");
                queue.remove(message);
                this.roundRobinLeader.setOutputStream(message.getRequestID(), null);
            }
            catch (Exception e) {
                logger.severe(Util.getStackTrace(e) + "\n to server ID " + workerAddress.toString());
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

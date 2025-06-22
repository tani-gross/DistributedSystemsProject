package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

public class JavaRunnerFollower extends Thread implements LoggingServer {
    private final PeerServerImpl peerServer;
    private volatile boolean shutdown;
    private Logger logger;
    private final int tcpPort;
    private ServerSocket serverSocket;
    private ConcurrentLinkedQueue<Message> workResultQueue = new ConcurrentLinkedQueue<>();

    public JavaRunnerFollower(PeerServerImpl peerServer, int tcpPort) {
        this.peerServer = peerServer;
        this.shutdown = false;
        this.tcpPort = tcpPort;

        try {
            this.logger = initializeLogging(JavaRunnerFollower.class.getCanonicalName() + "-on-port-" + this.peerServer.getUdpPort());
        } catch (Exception e) {
            System.err.println("Failed to initialize logger for JavaRunnerFollower: " + e.getMessage());
        }
    }

    public void shutdown() {
        this.shutdown = true;
        interrupt();
        if (serverSocket != null && !serverSocket.isClosed()) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                logger.warning("Error closing server socket: " + e.getMessage());
            }
        }

        logger.fine("JavaRunnerFollower on server " + peerServer.getServerId() + " is shutting down.");
    }
    
    @Override
    public void run() {
        logger.fine("JavaRunnerFollower on server " + peerServer.getServerId() + " started and listening for TCP connections on port " + tcpPort);

        try {
            this.serverSocket = new ServerSocket(tcpPort);
            while (!shutdown && !Thread.currentThread().isInterrupted()) {
                Socket clientSocket = serverSocket.accept();
                handleClient(clientSocket);
            }
        } catch (IOException e) {
            if (shutdown) {
                logger.fine("JavaRunnerFollower TCP server shut down.");
            } else {
                logger.severe("Error in JavaRunnerFollower TCP server: " + e.getMessage());
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }finally {
            if (serverSocket != null && !serverSocket.isClosed()) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    logger.warning("Error closing server socket: " + e.getMessage());
                }
            }
        }
    }

    public ConcurrentLinkedQueue<Message> getWorkQueue(){
        return this.workResultQueue;
    }

    private void handleClient(Socket clientSocket) throws ClassNotFoundException {
        try{
            byte[] requestBytes = Util.readAllBytesFromNetwork(clientSocket.getInputStream());
            if(requestBytes.length == 0){
                return;
            }
            Message workMessage = new Message(requestBytes);

            if(workMessage.getMessageType() == MessageType.NEW_LEADER_GETTING_LAST_WORK){
                this.handleCache(workMessage, clientSocket);
                return;
            }

            logger.fine("Received this message:\n" + workMessage);
            if (workMessage.getMessageType() != Message.MessageType.WORK) {
                return;
            }

            String javaCode = new String(workMessage.getMessageContents());

            JavaRunner javaRunner = new JavaRunner();
            String result;
            try {
                result = javaRunner.compileAndRun(new ByteArrayInputStream(javaCode.getBytes()));
                logger.fine("JavaRunner execution succeeded with result: " + result);
            } catch (Exception e) {
                result = "Error: " + e.getMessage();
                logger.warning("JavaRunner execution failed: " + e.getMessage());
            }

            Message resultMessage = new Message(
                    Message.MessageType.COMPLETED_WORK,
                    result.getBytes(),
                    peerServer.getAddress().getHostName(),
                    tcpPort,
                    workMessage.getSenderHost(),
                    workMessage.getSenderPort(),
                    workMessage.getRequestID()
                );

            this.workResultQueue.add(resultMessage);
                        
            //leader is dead
            if(peerServer.isPeerDead(workMessage.getReceiverPort() - 2)){
                this.logger.fine("Leader is dead, not forwarding to leader");
                //if the new leader is going to be this one
                if(peerServer.getCurrentLeader().getProposedLeaderID() == peerServer.getServerId()){
                    this.logger.fine("I will be the new leader");
                    peerServer.setSelfQueue(resultMessage);
                    //this.logger.fine("i am the new leader, setting the queue to have my message");
                    return;
                }

            //leader is alive, send back to the round robin leader
            }else{
                logger.fine("Sending COMPLETED_WORK back to the leader");
                clientSocket.getOutputStream().write(resultMessage.getNetworkPayload());
                clientSocket.getOutputStream().flush();
            }
            try{
                clientSocket.close();
            }catch (Exception e){
                e.printStackTrace();    
            }

        } catch (IOException e) {
            logger.severe("Error handling client connection: " + e.getMessage());
        }
    }

    private void handleCache(Message leaderMessage, Socket socket){
        try {
            if(this.getWorkQueue().size() == 0){
                Message outGoingMessage = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, "back".getBytes(), peerServer.getAddress().getHostName(), peerServer.getUdpPort() + 2, leaderMessage.getSenderHost(), leaderMessage.getSenderPort() + 2);
                socket.getOutputStream().write(outGoingMessage.getNetworkPayload());
                socket.getOutputStream().flush();
                return;
            }
            
            for(Message message : this.getWorkQueue()){
                Message updatedMessage = new Message(
                    message.getMessageType(),
                    message.getMessageContents(),
                    message.getSenderHost(),
                    message.getSenderPort(),
                    message.getSenderHost(),
                    leaderMessage.getSenderPort(),
                    message.getRequestID()
                );
                
                socket.getOutputStream().write(updatedMessage.getNetworkPayload());
                socket.getOutputStream().flush();
                logger.fine("Sent this message to the new leader from the queue: " + updatedMessage);
                this.getWorkQueue().remove(message);
            }
        
        } catch (IOException e) {
            logger.fine("Error while sending work to the leader: " + e.getMessage());
        }
    }
}
package edu.yu.cs.com3800.stage5;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Message.MessageType;

public class Gossiper extends Thread implements LoggingServer{
    private static final int GOSSIP = 2000;
    private static final int FAIL = GOSSIP * 15;
    private static final int CLEANUP = FAIL * 2;
    private final PeerServerImpl server;
    private volatile boolean running = true;
    private Logger summaryLogger;
    private Logger verboseLogger;
    private int myPort;
    private final ExecutorService messageHandler;
    private long heartbeatCounter = 0;
    private Map<Integer, Long> heartbeatTable = new ConcurrentHashMap<>();
    private final Map<Integer, Long> lastReceivedTimestamps = new ConcurrentHashMap<>();
    private final Map<Integer, Boolean> failedNodes = new ConcurrentHashMap<>();
    

    public Gossiper(PeerServerImpl server, int myPort) throws IOException{
        this.server = server;
        this.myPort = myPort;
        this.summaryLogger = initializeLogging(PeerServerImpl.class.getCanonicalName() + "-gossip-summary-on-port-" + this.myPort);
        this.verboseLogger = initializeLogging(PeerServerImpl.class.getCanonicalName() + "-gossip-verbose-on-port-" + this.myPort);
        this.messageHandler = Executors.newSingleThreadExecutor();
    }

    public void shutdown(){
        this.running = false;
        this.interrupt();
        this.messageHandler.shutdownNow();
    }

    @Override
    public void run() {
        long lastGossipTime = System.currentTimeMillis();

        messageHandler.submit(this::handleIncomingGossipMessages);

        while (running && !Thread.currentThread().isInterrupted()) {
            if (!server.isElectionInProgress() && System.currentTimeMillis() - lastGossipTime >= GOSSIP) {
                sendGossip();
                lastGossipTime = System.currentTimeMillis();
            }
            if(!server.isElectionInProgress()){
                detectAndHandleFailures();
            }
            
            try {
                Thread.sleep(GOSSIP);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void sendGossip(){
        this.incrementHeartbeatCounter();
        this.getHeartbeatTable().put(this.myPort, this.getHeartbeatCounter());
        byte[] gossipData;
        try {
            gossipData = this.serializeHeartbeatTable(this.heartbeatTable);
        } catch (IOException e) {
            this.summaryLogger.warning("Error serializing heartbeat table: " + e.getMessage());
            return;
        }
       
        
        Long[] peerIDs = server.getPeerIDtoAddress().keySet().stream()
                           .filter(id -> !id.equals(server.getServerId()))
                           .toArray(Long[]::new);

        if (peerIDs.length > 0) {
            Random rand = new Random();
            Long randomPeer = peerIDs[rand.nextInt(peerIDs.length)];
            server.sendMessage(MessageType.GOSSIP, gossipData, server.getPeerByID(randomPeer));
        }
    }
    
    private void handleIncomingGossipMessages() {
        while (running && !Thread.currentThread().isInterrupted()) {
            Message gossipMessage = server.getIncomingMessages().poll();
            if (gossipMessage != null && gossipMessage.getMessageType() == Message.MessageType.GOSSIP) {
                this.processGossipMessage(gossipMessage);
            }else if(gossipMessage != null && gossipMessage.getMessageType() != MessageType.GOSSIP){
                server.getIncomingMessages().add(gossipMessage);
            }
            
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void processGossipMessage(Message gossipMessage) {
        try {
            Map<Integer, Long> receivedTable = this.deserializeHeartbeatTable(gossipMessage.getMessageContents());
            this.mergeHeartbeatData(receivedTable, gossipMessage);
        } catch (IOException | ClassNotFoundException e) {
            this.summaryLogger.warning("Failed to deserialize gossip message: " + e.getMessage());
        }
    }

    private void detectAndHandleFailures(){
        long currentTime = System.currentTimeMillis();

        for (Map.Entry<Integer, Long> entry : this.lastReceivedTimestamps.entrySet()) {
            Integer port = entry.getKey();
            Long lastTimestamp = entry.getValue();
            Long timeDifference = currentTime - lastTimestamp;

            if(!port.equals(server.getServerId()) && !failedNodes.containsKey(port) && timeDifference > FAIL){
                failedNodes.put(port, true);
                this.server.reportFailedPeer(port);
                this.summaryLogger.fine(myPort + ": no heartbeat from server " + port + " - SERVER FAILED");
                System.out.println(myPort + ": no heartbeat from server " + port + " - SERVER FAILED");
            }
            
            if(failedNodes.getOrDefault(port, false) && timeDifference > CLEANUP + FAIL){
                this.heartbeatTable.remove(port);
                this.lastReceivedTimestamps.remove(port);
                this.failedNodes.remove(port);
                this.summaryLogger.fine("Node " + port + " removed after cleanup");
            }
        }
    }

    private byte[] serializeHeartbeatTable(Map<Integer, Long> table) throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (ObjectOutputStream objectStream = new ObjectOutputStream(byteStream)) {
            objectStream.writeObject(table);
            objectStream.flush();
        }
        return byteStream.toByteArray();
    }

    private Map<Integer, Long> deserializeHeartbeatTable(byte[] data) throws IOException, ClassNotFoundException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
        ObjectInputStream objectStream = new ObjectInputStream(byteStream);
        return (Map<Integer, Long>) objectStream.readObject();
    }

    private void mergeHeartbeatData(Map<Integer, Long> receivedTable, Message gossipMessage) {
        long currentTime = System.currentTimeMillis();

        this.verboseLogger.fine(myPort + ": received " + receivedTable + " from " + gossipMessage.getSenderPort() + " at time " + currentTime);
    
        for (Map.Entry<Integer, Long> entry : receivedTable.entrySet()) {
            Integer port = entry.getKey();
            Long receivedHeartbeat = entry.getValue();

            Long currentHeartbeat = this.heartbeatTable.getOrDefault(port, -1L);


            if(failedNodes.getOrDefault(port, false)){
                continue;
            }
            

            if (receivedHeartbeat > currentHeartbeat) {
                this.heartbeatTable.put(port, receivedHeartbeat);
                this.lastReceivedTimestamps.put(port, currentTime);
                this.summaryLogger.fine(myPort + ": updated " + port + "'s heartbeat sequence to " + receivedHeartbeat + " based on message from " + gossipMessage.getSenderPort() + " at node time " + currentTime);
            }
        }
    }

    private synchronized void incrementHeartbeatCounter() {
        this.heartbeatCounter++;
    }

    private synchronized long getHeartbeatCounter() {
        return this.heartbeatCounter;
    }

    private Map<Integer, Long> getHeartbeatTable() {
        return this.heartbeatTable;
    }

    public synchronized void resetTimeMaps() {
        this.lastReceivedTimestamps.clear();
    }

    public void gossipLog(String message){
        this.summaryLogger.fine(message);
    }

}

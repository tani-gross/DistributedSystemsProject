package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class PeerServerImpl extends Thread implements PeerServer, LoggingServer {
    private final int tcpPort;
    private UDPMessageSender senderWorker;
    private UDPMessageReceiver receiverWorker;
    private final InetSocketAddress myAddress;
    private final int myPort;
    private ServerState state;
    private volatile boolean shutdown;
    private LinkedBlockingQueue<Message> outgoingMessages;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Long id;
    private final Long gatewayID;
    private final int numberOfObservers;
    private long peerEpoch;
    private volatile Vote currentLeader;
    private Map<Long, InetSocketAddress> peerIDtoAddress;
    private Logger logger;
    private final ExecutorService tcpHandlerPool;
    private Gossiper gossiperThread;
    private volatile boolean electionInProgress = true;
    private final Map<Long, Boolean> failedPeers = new ConcurrentHashMap<>();
    private LinkedBlockingQueue<Message> selfQueue = new LinkedBlockingQueue<>();
    private int httpPort;
    private HttpServer httpServer;
    public JavaRunnerFollower followerWorker;
    public RoundRobinLeader leaderWorker;

    public PeerServerImpl(int udpPort, long peerEpoch, Long serverID, Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        super();
        this.myPort = udpPort;
        this.peerEpoch = peerEpoch;
        this.id = serverID;
        this.gatewayID = gatewayID;
        this.peerIDtoAddress = peerIDtoAddress;
        this.numberOfObservers = numberOfObservers;
        this.myAddress = new InetSocketAddress("localhost", udpPort);
        this.state = ServerState.LOOKING;
        this.shutdown = false;
        this.outgoingMessages = new LinkedBlockingQueue<>();
        this.incomingMessages = new LinkedBlockingQueue<>();
        this.tcpPort = this.myPort + 2;
        this.logger = initializeLogging(PeerServerImpl.class.getCanonicalName() + "-on-port-" + this.myPort);
        int numCores = Runtime.getRuntime().availableProcessors();
        this.tcpHandlerPool = Executors.newFixedThreadPool(numCores * 2); 
        this.httpPort = this.myPort + 5;
    }



    @Override
    public void shutdown() {
        this.shutdown = true;
        if (this.senderWorker != null) {
            this.senderWorker.shutdown();
        }
        if (this.receiverWorker != null) {
            this.receiverWorker.shutdown();
        }
        if (this.followerWorker != null) {
            this.followerWorker.shutdown();
        }
        if (this.leaderWorker != null) {
            this.leaderWorker.shutdown();
        }
        if (this.tcpHandlerPool != null) {
            this.tcpHandlerPool.shutdownNow();
        }
        if(this.gossiperThread != null){
            this.gossiperThread.shutdown();
        }

        
        this.logger.fine("Server on port " + myPort + " is shutting down.");
    }

    @Override
    public void run() {
        try {
            this.senderWorker = new UDPMessageSender(outgoingMessages, myPort);
            this.senderWorker.setDaemon(true);
            this.senderWorker.start();

            this.receiverWorker = new UDPMessageReceiver(incomingMessages, myAddress, myPort, this);
            this.receiverWorker.setDaemon(true);
            this.receiverWorker.start();

            if(this.gossiperThread == null){
                this.gossiperThread = new Gossiper(this, myPort);
                this.gossiperThread.setDaemon(true);
                this.gossiperThread.start();
            }

            this.startHttpServer();

            while (!this.shutdown) {
                switch (getPeerState()) {
                    case LOOKING:
                        this.logger.fine("Server on port " + myPort + " started leader election");
                        LeaderElection election = new LeaderElection(this, incomingMessages, this.logger);
                        Vote electionWinner = election.lookForLeader();

                        setCurrentLeader(electionWinner);
                        if (electionWinner.getProposedLeaderID() == this.id) {
                            setPeerState(ServerState.LEADING);
                        } else {
                            setPeerState(ServerState.FOLLOWING);
                        }
                        this.electionInProgress = false;
                        break;
                        
                    case LEADING:
                        if(followerWorker != null){
                            this.followerWorker.shutdown();
                            this.followerWorker = null;
                        }

                        if (leaderWorker == null) {
                            this.logger.fine("Server on port " + myPort + " is leading.");
                            leaderWorker = new RoundRobinLeader(peerIDtoAddress, this, gatewayID);
                            leaderWorker.setDaemon(true);
                            leaderWorker.start();
                        }

                        break;
                    case FOLLOWING:
                        if (followerWorker == null) {
                            this.logger.fine("Server on port " + myPort + " is following.");
                            followerWorker = new JavaRunnerFollower(this, tcpPort);
                            followerWorker.setDaemon(true);
                            followerWorker.start();
                        }
                        break;
                    default:
                        runGatewayServer();
                }
            
            }
        } catch (Exception e) {
            this.logger.warning("Exception in server loop: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void setCurrentLeader(Vote v) throws IOException {
        this.currentLeader = v;

        if (this.gossiperThread != null) {
            this.gossiperThread.resetTimeMaps();
        }
    }

    @Override
    public Vote getCurrentLeader() {
        return this.currentLeader;
    }

    @Override
    public void sendMessage(Message.MessageType type, byte[] messageContents, InetSocketAddress target) throws IllegalArgumentException {
        if (!peerIDtoAddress.containsValue(target)) {
            throw new IllegalArgumentException("Target address is not part of the cluster: " + target);
        }
        try {
            Message message = new Message(type, messageContents, myAddress.getHostName(), myAddress.getPort(), target.getHostName(), target.getPort());
            if (type == Message.MessageType.WORK || type == Message.MessageType.COMPLETED_WORK) {
                sendOverTCP(message, target);
            } else {
                outgoingMessages.put(message);
            }
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to send message over TCP", e);
        }catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException("Interrupted while sending message to outgoing queue", e);
        }
    }

    private void sendOverTCP(Message message, InetSocketAddress target) throws IOException {
        try (Socket socket = new Socket(target.getHostName(), target.getPort() + 2)) {
            socket.getOutputStream().write(message.getNetworkPayload());
            socket.getOutputStream().flush();
            socket.close();
        }
    }

    @Override
    public void sendBroadcast(Message.MessageType type, byte[] messageContents) {
        for (InetSocketAddress address : peerIDtoAddress.values()) {
            if(!isPeerDead(address)){
                try {
                    sendMessage(type, messageContents, address);
    
                } catch (IllegalArgumentException e) {
                    logger.warning("Failed to send broadcast message to " + address + ": " + e.getMessage());
                }
            }
        }
    }

    @Override
    public ServerState getPeerState() {
        return this.state;
    }

    @Override
    public void setPeerState(ServerState newState) {
        if(this.state == newState){
            return;
        }

        if(this.gossiperThread != null){
            this.gossiperThread.gossipLog(this.myPort + ": switching from " + this.state + " to " + newState);
        }
        System.out.println(this.myPort + ": switching from " + this.state + " to " + newState);

        this.state = newState;
    }

    @Override
    public Long getServerId() {
        return this.id;
    }

    @Override
    public long getPeerEpoch() {
        return this.peerEpoch;
    }

    @Override
    public InetSocketAddress getAddress() {
        return this.myAddress;
    }

    @Override
    public int getUdpPort() {
        return this.myPort;
    }

    @Override
    public InetSocketAddress getPeerByID(long peerId) {
        return peerIDtoAddress.get(peerId);
    }

    public void log(String message){
        this.logger.fine(message);
    }

    @Override
    public int getQuorumSize() {
        int nonObserverCount = peerIDtoAddress.size() - numberOfObservers;
        this.logger.fine("count: " + nonObserverCount);
        return (nonObserverCount / 2) + 1;
    }

    public Map<Long, InetSocketAddress> getPeerIDtoAddress() {
        return this.peerIDtoAddress;
    }

    protected LinkedBlockingQueue<Message> getIncomingMessages() {
        return this.incomingMessages;
    }

    private void runGatewayServer() throws IOException {
        logger.fine("GatewayPeerServerImpl running as OBSERVER");

        while (!Thread.currentThread().isInterrupted()) {
            if (this.electionInProgress) {
                logger.fine("Starting leader election.");
                LeaderElection election = new LeaderElection(this, getIncomingMessages(), this.logger);
                Vote electionWinner = election.lookForLeader();
    
                if (electionWinner != null) {
                    setCurrentLeader(electionWinner);
                    logger.fine("Leader elected: " + electionWinner.getProposedLeaderID());
                    this.electionInProgress = false;
                }
            }
    
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        logger.fine("Gateway has stopped");
    }

    public boolean isElectionInProgress() {
        return this.electionInProgress;
    }
        
    public void setElectionInProgress(boolean inProgress) {
        this.electionInProgress = inProgress;
    }

    public Long getGatewayId(){
        return this.gatewayID;
    }

    @Override
    public void reportFailedPeer(long peerPort){
        Long failedServerID = null;

        for(Map.Entry<Long, InetSocketAddress> entry : this.peerIDtoAddress.entrySet()){
            if(entry.getValue().getPort() == peerPort){
                failedServerID = entry.getKey();
                break;
            }
        }
        if(failedServerID == null){
            return;
        }

        if(failedServerID == gatewayID){
            return;
        }

        this.failedPeers.put(peerPort, true);
        
        this.peerIDtoAddress.remove(failedServerID);
        this.handlePeerFailure(peerPort, failedServerID);
    }

    @Override
    public boolean isPeerDead(long peerID){
        return this.failedPeers.getOrDefault(peerID, false);
    }

    @Override
    public boolean isPeerDead(InetSocketAddress address) {
        for (Map.Entry<Long, InetSocketAddress> entry : peerIDtoAddress.entrySet()) {
            if (entry.getValue().equals(address)) {
                return isPeerDead(entry.getValue().getPort());
            }
        }
        return false;
    }

    public Map<Long, Boolean> getFailedPeers(){
        return this.failedPeers;
    }

    private void handlePeerFailure(long port, Long failedId){
        if(currentLeader != null && currentLeader.getProposedLeaderID() == failedId){
            this.peerEpoch += 1;
            this.electionInProgress = true;

            if(this.getPeerState() == ServerState.FOLLOWING){
                this.setPeerState(ServerState.LOOKING);
            }
        }
    }

    public void setSelfQueue(Message message){
        this.selfQueue.add(message);
    }

    public LinkedBlockingQueue<Message> getSelfQueue(){
        return this.selfQueue;
    }

    private void startHttpServer() throws IOException{
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String summaryPath = "logs-" + date.format(formatter) + "/" + "edu.yu.cs.com3800.stage5.PeerServerImpl-gossip-summary-on-port-" + myPort + "-Log.txt";
        String verbosePath = "logs-" + date.format(formatter) + "/" + "edu.yu.cs.com3800.stage5.PeerServerImpl-gossip-verbose-on-port-" + myPort + "-Log.txt";

        InetSocketAddress httpAddress = new InetSocketAddress(httpPort);
        this.httpServer = HttpServer.create(httpAddress, 50);
        this.httpServer.createContext("/logs/summary", new LogHandler(summaryPath));
        this.httpServer.createContext("/logs/verbose", new LogHandler(verbosePath));
        this.httpServer.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r);
            t.setDaemon(true);
            return t;
        }));
        this.httpServer.start();
    }

    private class LogHandler implements HttpHandler{
        private String filePath;

        public LogHandler(String filePath){
            this.filePath = filePath;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException{
            File logFile = new File(filePath);
            byte[] content = Files.readAllBytes(logFile.toPath());
            exchange.getResponseHeaders().set("Content-Type", "text/plain");
            exchange.sendResponseHeaders(200, content.length);
            try(OutputStream os = exchange.getResponseBody()){
                os.write(content);
                os.flush();
            }
        }

    }

    public static void main(String[] args) throws IOException{
        int port = Integer.parseInt(args[0]);
        Long serverID = Long.parseLong(args[1]);
        int numberOfObservers = Integer.parseInt(args[2]);
        int clusterSize = Integer.parseInt(args[3]);

        Map<Long, InetSocketAddress> peerIDtoAddress = new HashMap<>();
        peerIDtoAddress.put(0L, new InetSocketAddress("localhost", 8000));

        for(int i = 1; i < clusterSize; i++){
            if(i != serverID){
                peerIDtoAddress.put((long)i, new InetSocketAddress("localhost", 8000 + (i * 10)));
            }
        }

        PeerServerImpl peerServer = new PeerServerImpl(port, 0, serverID, peerIDtoAddress, 0L, 1);
        peerServer.start();
    }

    public RoundRobinLeader getRoundRobin(){
        return this.leaderWorker;
    }

    public JavaRunnerFollower getJavaWorker(){
        return this.followerWorker;
    }

}
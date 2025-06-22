package edu.yu.cs.com3800.stage5;

import com.sun.net.httpserver.HttpServer;
import edu.yu.cs.com3800.LoggingServer;
import edu.yu.cs.com3800.Message;
import edu.yu.cs.com3800.Util;
import edu.yu.cs.com3800.Vote;

import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class GatewayServer extends Thread implements LoggingServer {
    private final HttpServer httpServer;
    private final GatewayPeerServerImpl gatewayPeerServer;
    private final ConcurrentHashMap<Integer, byte[]> cache;
    private int numberOfObservers;
    private final Logger logger;
    private int peerPort;
    private final ExecutorService requestHandlerPool;
    private volatile int queueSize = 0;

    public GatewayServer(int httpPort, int peerPort, long peerEpoch, Long serverID, 
                         ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress, int numberOfObservers) throws IOException {
        super("GatewayServer-Thread");
        this.httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        this.cache = new ConcurrentHashMap<>();
        this.logger = initializeLogging(GatewayServer.class.getCanonicalName() + "-on-port-" + peerPort);
        this.numberOfObservers = numberOfObservers;

        this.gatewayPeerServer = new GatewayPeerServerImpl(peerPort, peerEpoch, serverID, peerIDtoAddress, 0L, this.numberOfObservers);
        this.httpServer.createContext("/compileandrun", new RequestHandler());
        this.httpServer.createContext("/cluster-info", new RequestHandler());
        this.httpServer.setExecutor(Executors.newCachedThreadPool());

        this.peerPort = peerPort;

        this.requestHandlerPool = Executors.newCachedThreadPool();
    }

    @Override
    public void run() {
        this.gatewayPeerServer.start();
        this.httpServer.start();
        logger.fine("GatewayServer started and running.");

        while (!Thread.currentThread().isInterrupted()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        shutdown();
    }


    public String getCurrentLeader() {
        return this.gatewayPeerServer.getCurrentLeaderHost();
    }

    public void shutdown(){
        this.gatewayPeerServer.shutdown();
        this.httpServer.stop(0);
        logger.fine("GatewayServer stopped.");
    }

    private class RequestHandler implements HttpHandler {
        private final AtomicInteger requestIdCounter = new AtomicInteger(0);
        private int count = 0;
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            requestHandlerPool.submit(() -> {
                try {
                    int requestId = requestIdCounter.getAndIncrement();
                    processRequest(exchange, logger, gatewayPeerServer, requestId);
                } catch (IOException e) {
                    //logger.warning("Error processing request: " + e.getMessage());

                }
            });
        }

        private void processRequest(HttpExchange exchange, Logger logger, GatewayPeerServerImpl gatewaypeerServer, int requestIdCounter) throws IOException{
            if ("GET".equals(exchange.getRequestMethod())) {
                this.demoHandler(exchange);
            }
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.getResponseHeaders().add("Cached-Response", "false");
                exchange.sendResponseHeaders(405, -1);
                return;
            }
            Headers headers = exchange.getRequestHeaders();
            String contentType = headers.getFirst("Content-Type");

            if (!"text/x-java-source".equals(contentType)) {
                exchange.getResponseHeaders().add("Cached-Response", "false");
                exchange.sendResponseHeaders(400, -1);
                return;
            }
            byte[] requestData = Util.readAllBytes(exchange.getRequestBody());

            if (requestData.length == 0) {
                logger.warning("Request body is empty!");
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            int requestHash = Arrays.hashCode(requestData);
            if (cache.containsKey(requestHash)) {
                sendCachedResponse(exchange, cache.get(requestHash));
            } else {
                forwardToLeader(exchange, requestHash, requestData, logger, gatewayPeerServer, requestIdCounter);
            }
        }

        
        private void forwardToLeader(HttpExchange exchange, int requestHash, byte[] requestData, Logger logger, GatewayPeerServerImpl gatewayPeerServer, int requestIdCounter) throws IOException {
            this.count += 1;
            String leaderHost = gatewayPeerServer.getCurrentLeaderHost();
            int leaderTcpPort = gatewayPeerServer.getCurrentLeaderTcpPort();
            
            try (Socket socket = new Socket(leaderHost, leaderTcpPort)) {                
                Message message = new Message(
                    Message.MessageType.WORK,
                    requestData,
                    gatewayPeerServer.getAddress().getHostName(),
                    gatewayPeerServer.getAddress().getPort() + 2,
                    leaderHost,
                    leaderTcpPort,
                    requestIdCounter,
                    false
                );
                logger.fine("Sending this message to the leader:\n" + message);
                socket.getOutputStream().write(message.getNetworkPayload());
                socket.getOutputStream().flush();

                byte[] leaderResponse = Util.readAllBytesFromNetwork(socket.getInputStream());

                while(leaderResponse.length == 0 && !gatewayPeerServer.isPeerDead(leaderTcpPort-2)){
                    leaderResponse = Util.readAllBytesFromNetwork(socket.getInputStream());
                }

                if(gatewayPeerServer.isPeerDead(leaderTcpPort-2)){
                    logger.fine("Leader is dead. Waiting for new leader");
                    throw new Exception("Leader is dead. Waiting for new leader");
                }

                Message leaderMessage = new Message(leaderResponse);
                    
                logger.fine("Received this message from leader:\n" + leaderMessage);
    
                cache.put(requestHash, leaderResponse);
    
                byte[] responseBody = leaderMessage.getMessageContents();
    
                exchange.getResponseHeaders().add("Cached-Response", "false");
                exchange.sendResponseHeaders(200, responseBody.length);
    
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(responseBody);
                }

                socket.close();
            } catch (Exception e) {
                this.waitForNewLeader(exchange, requestHash, requestData, logger, gatewayPeerServer, leaderTcpPort-2, requestIdCounter);
            }
            
        }
        
        private void sendCachedResponse(HttpExchange exchange, byte[] cachedResponse) throws IOException {
            Message cachedMessage = new Message(cachedResponse);
        
            byte[] responseBody = cachedMessage.getMessageContents();
        
            logger.fine("Returning response from cache:\n" + cachedMessage);
        
            exchange.getResponseHeaders().add("Cached-Response", "true");
            exchange.sendResponseHeaders(200, responseBody.length);
        
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBody);
            }
        }

        private void waitForNewLeader(HttpExchange exchange, int requestHash, byte[] requestData, Logger logger, GatewayPeerServerImpl gatewayPeerServer, int oldLeaderPort, int requestIdCounter) throws IOException {
            logger.fine("Waiting for a new leader to be elected...");
            Long id = 0L;
            for (Map.Entry<Long, InetSocketAddress> entry : gatewayPeerServer.getPeerIDtoAddress().entrySet()) {
                if (entry.getValue().getPort() == oldLeaderPort) {
                    id = entry.getKey();
                }
            }

            Long newId = gatewayPeerServer.getCurrentLeader().getProposedLeaderID();
            while (id == newId) {
                newId = gatewayPeerServer.getCurrentLeader().getProposedLeaderID();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                    return;
                }
            }

            logger.fine("New leader detected: " + gatewayPeerServer.getCurrentLeaderTcpPort());

            this.forwardToLeader(exchange, requestHash, requestData, logger, gatewayPeerServer, requestIdCounter);

        }

        private void demoHandler(HttpExchange exchange) throws IOException{
            Vote currentLeader = gatewayPeerServer.getCurrentLeader();
            if(currentLeader == null){
                exchange.getResponseHeaders().add("Content-Type", "text/plain");
                exchange.sendResponseHeaders(200, "none".getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write("none".getBytes());
                os.close();
            }
            Long leaderID = gatewayPeerServer.getCurrentLeader().getProposedLeaderID();
            Map<Long, InetSocketAddress> peers = gatewayPeerServer.getPeerIDtoAddress();
            StringBuilder responseBuilder = new StringBuilder();

            for (Map.Entry<Long, InetSocketAddress> entry : peers.entrySet()) {
                Long peerID = entry.getKey();
                InetSocketAddress address = entry.getValue();
                int port = address.getPort();


                if(!gatewayPeerServer.getFailedPeers().containsKey((long)port)){
                    if (peerID.equals(leaderID)) {
                        responseBuilder.append("LEADER - ").append(port).append("\n");
                    } else {
                        responseBuilder.append("FOLLOWER - ").append(port).append("\n");
                    }
                }
            }

            String response = responseBuilder.toString();

            // Set response headers
            exchange.sendResponseHeaders(200, response.getBytes().length);

            // Write response body
            OutputStream os = exchange.getResponseBody();
            os.write(response.getBytes());
            os.close();
        }
        
    }

    public int getQueuedSize(){
        return this.queueSize;
    }

    public GatewayPeerServerImpl getPeerServer(){
        return this.gatewayPeerServer;
    } 

    public static void main(String[] args) throws IOException{
        int httpPort = Integer.parseInt(args[0]);
        int peerPort = Integer.parseInt(args[1]);
        int clusterSize = Integer.parseInt(args[2]);

        ConcurrentHashMap<Long, InetSocketAddress> peerIDtoAddress = new ConcurrentHashMap<>();

        for (long i = 1; i < clusterSize; i++) {
            int assignedPort = 8000 + (int) i * 10;
            peerIDtoAddress.put(i, new InetSocketAddress("localhost", assignedPort));
        }

        GatewayServer gatewayServer = new GatewayServer(httpPort, peerPort, 0, 0L, peerIDtoAddress, 1);
        gatewayServer.start();
    }
}
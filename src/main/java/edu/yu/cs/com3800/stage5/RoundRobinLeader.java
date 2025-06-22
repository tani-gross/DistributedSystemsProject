package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import edu.yu.cs.com3800.Message.MessageType;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.BufferUnderflowException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

public class RoundRobinLeader extends Thread implements LoggingServer {
    private final Map<Long, InetSocketAddress> peerIDtoAddress;
    private final PeerServerImpl peerServer;
    private final Logger logger;
    private final ServerSocket serverSocket;
    private final ThreadPoolExecutor threadPool;
    private volatile boolean shutdown = false;
    private int currentWorkerIndex = 0;
    private final long gatewayID;
    private Map<Long, Message> queueMessages = new ConcurrentHashMap<>();
    private final ExecutorService cacheHandler;
    private volatile boolean checkedCaches = false;

    public RoundRobinLeader(Map<Long, InetSocketAddress> peerIDtoAddress, PeerServerImpl peerServer, long gatewayID) throws IOException {
        
        this.peerIDtoAddress = peerIDtoAddress;
        this.peerServer = peerServer;
        this.gatewayID = gatewayID;

        int tcpPort = peerServer.getUdpPort() + 2;

        this.serverSocket = new ServerSocket(tcpPort);
        
        this.threadPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        this.logger = initializeLogging(RoundRobinLeader.class.getCanonicalName() + "-on-port-" + (tcpPort-2));
        this.cacheHandler = Executors.newSingleThreadExecutor();
    }

    @Override
    public void run() {
        if(this.peerServer.getSelfQueue().size() != 0){
            Message queueMessage = this.peerServer.getSelfQueue().poll();
            this.queueMessages.put(queueMessage.getRequestID(), queueMessage);
        }

        cacheHandler.submit(this::doWorkerCacheMessages);

        List<Long> workerIds = new ArrayList<>();
        for (Long id : peerIDtoAddress.keySet()) {
            if (id == peerServer.getServerId() || id == gatewayID) {
                continue;
            }
            workerIds.add(id);
        }

        this.logger.fine("RoundRobinLeader started. Listening on port: " + serverSocket.getLocalPort());

        while (!shutdown) {
            try {
                Socket gatewaySocket = serverSocket.accept();
                threadPool.submit(() -> handleGatewayRequest(gatewaySocket, workerIds));
            } catch (IOException e) {
                if (!shutdown) {
                    logger.warning("Error accepting connection from GatewayServer: " + e.getMessage());
                }
            }
        }
        this.logger.fine("RoundRobinLeader shutting down.");
        
    }


    private void handleGatewayRequest(Socket gatewaySocket, List<Long> workerIds){
        while(!this.checkedCaches){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
        }

        try (InputStream inputStream = gatewaySocket.getInputStream()) {
            byte[] requestData = Util.readAllBytesFromNetwork(inputStream);
            while(requestData.length == 0){
                requestData = Util.readAllBytesFromNetwork(inputStream);
            }

            Message gatewayMessage = new Message(requestData);
            boolean workHandled = false;

            if(this.checkWorkerQueues(gatewayMessage.getRequestID())){
                this.logger.fine("Receieved queued message from a worker: " + gatewayMessage);
                gatewaySocket.getOutputStream().write(gatewayMessage.getNetworkPayload());
                gatewaySocket.getOutputStream().flush();

                return;
                
            }

            this.logger.fine("Received message from GatewayServer:\n" + gatewayMessage);

            while(!workHandled && !workerIds.isEmpty()){
                Long workerId = workerIds.get(currentWorkerIndex);

                InetSocketAddress workerAddress = peerIDtoAddress.get(workerId);
                
                currentWorkerIndex = (currentWorkerIndex + 1) % workerIds.size();
                Message responseMessage = forwardToWorker(gatewayMessage, workerAddress);
                if (responseMessage != null) {
                    this.logger.fine("Fowarded message to worker ID: " + workerId + " at address: " + workerAddress);
                    this.logger.fine("Received response from worker:\n" + responseMessage);
                    gatewaySocket.getOutputStream().write(responseMessage.getNetworkPayload());
                    gatewaySocket.getOutputStream().flush();
                    
                    workHandled = true;
                } else {
                    workerIds.remove(workerId);
                    this.peerServer.reportFailedPeer(workerId);
                    if (workerIds.isEmpty()) {
                        this.logger.severe("No available workers to reassign work.");
                    } else {
                        this.logger.fine("No response received. Reassigning work to worker " + workerIds.get(currentWorkerIndex));
                    }
                }
            }


        } catch (IOException e) {
            this.logger.warning("Error handling GatewayServer request: " + e.getMessage());
        } finally {
            try {
                gatewaySocket.close();
            } catch (IOException e) {
                this.logger.warning("Error closing GatewayServer socket: " + e.getMessage());
            }
        }
    }

    public Message forwardToWorker(Message gatewayMessage, InetSocketAddress workerAddress) {
        try (Socket workerSocket = new Socket(workerAddress.getHostName(), workerAddress.getPort() + 2)) {
            workerSocket.getOutputStream().write(gatewayMessage.getNetworkPayload());
            workerSocket.getOutputStream().flush();

            byte[] responseData = Util.readAllBytesFromNetwork(workerSocket.getInputStream());

            while(responseData.length == 0 && !this.peerServer.isPeerDead(workerAddress.getPort())){
                responseData = Util.readAllBytesFromNetwork(workerSocket.getInputStream());
            }

            workerSocket.close();
            return new Message(responseData);

        } catch (Exception e) {
            return null;
        }
    }

    public void shutdown() {
        shutdown = true;
        try {
            serverSocket.close();
        } catch (IOException e) {
            logger.warning("Error closing ServerSocket: " + e.getMessage());
        }
        threadPool.shutdown();
        try {
            if (!threadPool.awaitTermination(5, TimeUnit.SECONDS)) {
                threadPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            threadPool.shutdownNow();

            Thread.currentThread().interrupt();
        }
        this.cacheHandler.shutdownNow();
    }

    private boolean checkWorkerQueues(Long messageId){
        for(Map.Entry<Long, Message> entry : this.queueMessages.entrySet()){
            if(entry.getKey() == messageId){
                return true;
            }
        }

        return false;
    }

    private void doWorkerCacheMessages(){
        for(InetSocketAddress address: this.peerIDtoAddress.values()){
            try (Socket workerSocket = new Socket(address.getHostName(), address.getPort() + 2)) {
                Message outGoingMessage = new Message(MessageType.NEW_LEADER_GETTING_LAST_WORK, "test".getBytes(), peerServer.getAddress().getHostName(), peerServer.getUdpPort() + 2, address.getHostName(), address.getPort() + 2);
                workerSocket.getOutputStream().write(outGoingMessage.getNetworkPayload());
                workerSocket.getOutputStream().flush();
                byte[] queueBytes = Util.readAllBytesFromNetwork(workerSocket.getInputStream());
                Message message = null;
                try{
                    message = new Message(queueBytes);
                }catch(BufferUnderflowException e){
                    while(queueBytes.length == 0){
                        queueBytes = Util.readAllBytesFromNetwork(workerSocket.getInputStream());
                    }
                    message = new Message(queueBytes);
                }
                
                if(message.getMessageType() == MessageType.NEW_LEADER_GETTING_LAST_WORK){
                    continue;
                }

                this.queueMessages.put(message.getRequestID(), message);
            }catch(IOException e){
                continue;
            }
        }
        this.checkedCaches = true;
    }
}

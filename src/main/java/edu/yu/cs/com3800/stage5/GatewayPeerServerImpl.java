package edu.yu.cs.com3800.stage5;

import edu.yu.cs.com3800.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;

public class GatewayPeerServerImpl extends PeerServerImpl{

    private int numberOfObservers;

    public GatewayPeerServerImpl(int peerPort, long peerEpoch, Long serverID, 
                             Map<Long, InetSocketAddress> peerIDtoAddress, Long gatewayID, int numberOfObservers) throws IOException {
        super(peerPort, peerEpoch, serverID, peerIDtoAddress, gatewayID, numberOfObservers);
        setPeerState(ServerState.OBSERVER);
        this.numberOfObservers = numberOfObservers;
    }

    public String getCurrentLeaderHost() {
        Vote currentLeader = getCurrentLeader();
        if (currentLeader != null) {
            InetSocketAddress leaderAddress = getPeerByID(currentLeader.getProposedLeaderID());
            return leaderAddress.getHostString();
        }
        return null;
    }

    public int getCurrentLeaderTcpPort() {
        Vote currentLeader = getCurrentLeader();
        if (currentLeader != null) {
            InetSocketAddress leaderAddress = getPeerByID(currentLeader.getProposedLeaderID());
            int tcpPort = leaderAddress.getPort() + 2;
            return tcpPort;
        }
        return -1;
    }

    @Override
    public void setPeerState(ServerState newState){
        if(newState != ServerState.OBSERVER){
            return;
        }

        super.setPeerState(newState);
    }

    @Override
    public int getQuorumSize() {
        log("count: " + getPeerIDtoAddress().size());
        return (getPeerIDtoAddress().size() / 2) + 1;
    }
}
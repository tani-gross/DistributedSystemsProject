package edu.yu.cs.com3800;

import edu.yu.cs.com3800.Message.MessageType;
import edu.yu.cs.com3800.PeerServer.ServerState;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**We are implemeting a simplfied version of the election algorithm. For the complete version which covers all possible scenarios, see https://github.com/apache/zookeeper/blob/90f8d835e065ea12dddd8ed9ca20872a4412c78a/zookeeper-server/src/main/java/org/apache/zookeeper/server/quorum/FastLeaderElection.java#L913
 */
public class LeaderElection {
    /**
     * time to wait once we believe we've reached the end of leader election.
     */
    private final static int finalizeWait = 3200;

    /**
     * Upper bound on the amount of time between two consecutive notification checks.
     * This impacts the amount of time to get the system up again after long partitions. Currently 30 seconds.
     */
    private final static int maxNotificationInterval = 30000;

    private PeerServer server;
    private LinkedBlockingQueue<Message> incomingMessages;
    private Logger logger;
    private long proposedLeader;
    private long proposedEpoch;

    public LeaderElection(PeerServer server, LinkedBlockingQueue<Message> incomingMessages, Logger logger) {
        this.server = server;
        this.incomingMessages = incomingMessages;
        this.logger = logger;
        this.proposedLeader = server.getServerId();
        this.proposedEpoch = server.getPeerEpoch();
    }

    /**
     * Note that the logic in the comments below does NOT cover every last "technical" detail you will need to address to implement the election algorithm.
     * How you store all the relevant state, etc., are details you will need to work out.
     * @return the elected leader
     */

     public synchronized Vote lookForLeader() {
        try {
            sendNotifications();
            int backoff = 1000;
            Map<Long, ElectionNotification> receivedVotes = new ConcurrentHashMap<>();
    
            // Loop in which we exchange notifications with other servers until we find a leader
            while (true) {
                Message message = incomingMessages.poll();
                
                if (message == null) {
                    //this.logger.fine("No message received, waiting for " + backoff + " ms.");
                    Thread.sleep(backoff);
                    sendNotifications();
                    backoff = Math.min(backoff * 2, maxNotificationInterval);
                    continue;
                }

                if(message.getMessageType() != MessageType.ELECTION){
                    continue;
                }
    
                ElectionNotification notification = parseNotification(message);
                //this.logger.fine("Received notification from server ID: " + notification.getSenderID() +
                //       ", proposing leader ID: " + notification.getProposedLeaderID() + ", epoch: " + notification.getPeerEpoch());
    
                if (notification.getState() == ServerState.OBSERVER) {
                    //this.logger.fine("Ignoring message from observer server ID: " + notification.getSenderID());
                    continue;
                }
    
                if (notification.getPeerEpoch() < this.proposedEpoch) {
                    //this.logger.fine("Ignoring message from earlier epoch: " + notification.getPeerEpoch());
                    continue;
                }

                if (server.isPeerDead(notification.getSenderID()) || server.isPeerDead(notification.getProposedLeaderID())) {
                    //this.logger.fine("Ignoring notification from or about dead node: " + notification.getSenderID());
                    continue;
                }
    
                if (supersedesCurrentVote(notification.getProposedLeaderID(), notification.getPeerEpoch())) {
                    //this.logger.fine("Updating my vote. Changing proposed leader from " + this.proposedLeader + " to " + notification.getProposedLeaderID());
                    this.proposedLeader = notification.getProposedLeaderID();
                    this.proposedEpoch = notification.getPeerEpoch();
                    sendNotifications();
                }
    
                receivedVotes.put(notification.getSenderID(), notification);
                //this.logger.fine("Votes received so far: " + receivedVotes.size());
    
                if (haveEnoughVotes(receivedVotes, new Vote(this.proposedLeader, this.proposedEpoch))) {
                    //this.logger.fine("Have enough votes to declare leader. Doing final check for higher-ranked votes.");
                    
                    boolean higherRankedVoteFound = false;
                    long finalCheckStartTime = System.currentTimeMillis();
    
                    while (System.currentTimeMillis() - finalCheckStartTime < finalizeWait) {
                        Message finalCheckMessage = incomingMessages.poll();
                        
                        if (finalCheckMessage == null || finalCheckMessage.getMessageType() != MessageType.ELECTION ) {
                            continue;
                        }
                        
                        ElectionNotification finalCheckNotification = parseNotification(finalCheckMessage);

                        if(server.isPeerDead(finalCheckNotification.getSenderID())){
                            continue;
                        }
                        //this.logger.fine("Final check received message from server ID: " + finalCheckNotification.getSenderID() +
                        //        ", proposing leader ID: " + finalCheckNotification.getProposedLeaderID() + ", epoch: " + finalCheckNotification.getPeerEpoch());
                        if (supersedesCurrentVote(finalCheckNotification.getProposedLeaderID(), finalCheckNotification.getPeerEpoch())) {
                            higherRankedVoteFound = true;
                            //this.logger.fine("Found higher-ranked vote from server ID: " + finalCheckNotification.getSenderID() +
                            //         ", proposing leader ID: " + finalCheckNotification.getProposedLeaderID());
                            break;
                        }
                    }
    
                    if (!higherRankedVoteFound) {
                        return acceptElectionWinner(new ElectionNotification(proposedLeader, server.getPeerState(), server.getServerId(), proposedEpoch));
                    }
                }
            }
        } catch (Exception e) {
            this.logger.log(Level.SEVERE, "Exception occurred during election; election canceled", e);
        }
    
        //this.logger.fine("Exiting leader election without a leader.");
        return null;
    }
    
    
     private void sendNotifications() {
        if (server.getPeerState() == ServerState.OBSERVER) {
            return;
        }

        byte[] messageContents = createNotificationMessage();
        server.sendBroadcast(Message.MessageType.ELECTION, messageContents);
    }

    private ElectionNotification parseNotification(Message message) {
        return LeaderElection.getNotificationFromMessage(message);
    }

    private Vote acceptElectionWinner(ElectionNotification n) {
        if (n.getProposedLeaderID() == server.getServerId()) {
            server.setPeerState(ServerState.LEADING);
        } else {
            server.setPeerState(ServerState.FOLLOWING);
        }
        incomingMessages.clear();
        //this.logger.fine("elected leader: " + n.getProposedLeaderID());
        return new Vote(n.getProposedLeaderID(), n.getPeerEpoch());
    }

    /*
     * We return true if one of the following three cases hold:
     * 1- New epoch is higher
     * 2- New epoch is the same as current epoch, but server id is higher.
     */
    protected boolean supersedesCurrentVote(long newId, long newEpoch) {
        return (newEpoch > this.proposedEpoch) || ((newEpoch == this.proposedEpoch) && (newId > this.proposedLeader));
    }

    /**
     * Termination predicate. Given a set of votes, determines if we have sufficient support for the proposal to declare the end of the election round.
     * Who voted for who isn't relevant, we only care that each server has one current vote.
     */
    protected boolean haveEnoughVotes(Map<Long, ElectionNotification> votes, Vote proposal) {
        long count = votes.values().stream()
                .filter(v -> v.getProposedLeaderID() == proposal.getProposedLeaderID())
                .count();
        return count >= server.getQuorumSize();
    }

    private byte[] createNotificationMessage() {
        ElectionNotification notification = new ElectionNotification(proposedLeader, server.getPeerState(), server.getServerId(), proposedEpoch);
        return LeaderElection.buildMsgContent(notification);
    }

    public static byte[] buildMsgContent(ElectionNotification notification) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES * 3 + Character.BYTES + notification.getState().name().length());
        buffer.putLong(notification.getProposedLeaderID());
        buffer.putChar(notification.getState().getChar());
        buffer.putLong(notification.getSenderID());
        buffer.putLong(notification.getPeerEpoch());
        return buffer.array();
    }
    

    public static ElectionNotification getNotificationFromMessage(Message message) {
        ByteBuffer buffer = ByteBuffer.wrap(message.getMessageContents());
        long proposedLeaderID = buffer.getLong();
        char stateChar = buffer.getChar();
        PeerServer.ServerState state = PeerServer.ServerState.getServerState(stateChar);
        long senderID = buffer.getLong();
        long peerEpoch = buffer.getLong();
        return new ElectionNotification(proposedLeaderID, state, senderID, peerEpoch);
    }
    
}
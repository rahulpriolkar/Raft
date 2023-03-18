package io.rahulpriolkar.raft;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class RaftRPCClient {
    private final RaftRPCServiceGrpc.RaftRPCServiceStub stub;
    private final Raft raftObject;

    public RaftRPCClient(Channel channel, Raft raftObject) {
        stub = RaftRPCServiceGrpc.newStub(channel);
        this.raftObject = raftObject;
    }

    public void appendEntries(AppendEntriesRequest request) {
        try {
            stub.appendEntries(request, new StreamObserver<AppendEntriesResponse>() {
                @Override
                public void onNext(AppendEntriesResponse value) {
                    // Case: Leader had gone offline, just came back online, and received an append entry from one of the
                    // other nodes, and updated its term as it was behind, and switched to follower state.

                    // Now should the rest of the appendEntries be ignored [??]
                    synchronized (raftObject) {
                        if(raftObject.getCurrentState() == Raft.NodeState.FOLLOWER) {
                            return;
                        }

                        // cancel initiate election // not really election is cancelled on service side
                        System.out.println("Append Entries Response: " + value.getSuccess());
                        if(!value.getSuccess()) {
                            if(value.getTerm() > raftObject.getCurrentTerm()) {
                                System.out.println("Converted to Follower state, Updated Current term to match the node who sent the append entry response");
                                raftObject.setCurrentState(Raft.NodeState.FOLLOWER);
                                raftObject.setCurrentTerm(value.getTerm());
                                raftObject.logger.info("(" + raftObject.getPORT() + "): Converted to Follower state. Updated Term to = " + value.getTerm());
                                raftObject.cancelAppendEntriesAll();

                                // Must call heartbeat as the node has been converted to a Follower
                                raftObject.heartbeat(false);
                            } else {
                                // decrement nextIndex for the follower node
                                try {
                                    HashMap<Integer, Integer> nextIndex = raftObject.getNextIndex();
                                    System.out.println("Before" + nextIndex.get((int)value.getSenderPort()));
//                                nextIndex.put((int)value.getSenderPort(), nextIndex.get((int)value.getSenderPort())-1);
                                    nextIndex.put((int) value.getSenderPort(), nextIndex.get((int)value.getSenderPort()) - 1);

                                    raftObject.setNextIndex(nextIndex);
                                    System.out.println("After" + nextIndex.get((int) value.getSenderPort()));
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        } else { // AppendEntries Success
//                             increment nextIndex by the number of log entries appended to the Follower's log
                            try {
                                HashMap<Integer, Integer> nextIndex = raftObject.getNextIndex();
                                // appendEntries hangs if the 2nd arg to "put" is not typecast to "int"
                                nextIndex.put((int)value.getSenderPort(), nextIndex.get((int)value.getSenderPort()) + request.getEntriesList().size());
                                raftObject.setNextIndex(nextIndex);

                                // set matchIndex = nextIndex for the Follower
                                HashMap<Integer, Integer> matchIndex = raftObject.getMatchIndex();
                                matchIndex.put((int)value.getSenderPort(), nextIndex.get((int)value.getSenderPort())-1);
                                raftObject.setMatchIndex(matchIndex);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }

                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println(t.getMessage());
//                    t.printStackTrace();
                }

                @Override
                public void onCompleted() {
//                    System.out.println("Completed");
                }
            });
        } catch(StatusRuntimeException e) {
            System.out.println(e.getMessage());
        }
    }

    public void requestVote(RequestVoteRequest request) {
        try {
            stub.withDeadlineAfter(this.raftObject.getElectionTimeout(), TimeUnit.SECONDS).requestVote(request, new StreamObserver<RequestVoteResponse>() {
                @Override
                public void onNext(RequestVoteResponse value) {

                    synchronized (raftObject) {
                        // Candidate received a valid append entry from a new leader and moved to FOLLOWER state
                        // Ignore the rest of the requestVote responses
                        if(raftObject.getCurrentState() == Raft.NodeState.FOLLOWER) {
                            return;
                        }

                        System.out.println("Received requestVote Response from " + value.getSenderPort() + "(term= " + value.getTerm() + ")" + " vote: " + value.getVoteGranted());
                        raftObject.logger.info("(" + raftObject.getPORT() + "): Received RequestVote Response from " + value.getSenderPort() + "(term= " + value.getTerm() + ")" + " vote: " + value.getVoteGranted() + " Total Votes = " + raftObject.getVotesReceived());

                        // Update currentTerm
                        if(value.getTerm() > raftObject.getCurrentTerm()) {
                            raftObject.setCurrentTerm(value.getTerm());
                            // return // can return here
                        }

                        // Add logic to check if the votes received are majority
                        if(value.getVoteGranted()) {
                            raftObject.setVotesReceived(raftObject.getVotesReceived() + 1);
                        }

                        if(raftObject.getCurrentState() != Raft.NodeState.LEADER && raftObject.getVotesReceived() > raftObject.getServerCount() / 2) {
                            // Declare current server the leader
                            System.out.println("ELECTED AS LEADER! current term = " + raftObject.getCurrentTerm());
                            raftObject.logger.info("(" + raftObject.getPORT() + ") : Elected As LEADER!");

                            // cancel ongoing/scheduled initiateElection after getting elected a leader
                            raftObject.setCurrentState(Raft.NodeState.LEADER);

                            raftObject.heartbeat(true);
                        }
                    }
                }

                @Override
                public void onError(Throwable t) {
                    System.out.println(t.getMessage());
                }

                @Override
                public void onCompleted() {
//                    System.out.println("Completed");
                }
            });
        } catch(StatusRuntimeException e) {
            System.out.println(e.getMessage());
        }
    }
}

package io.rahulpriolkar.raft;

import io.grpc.Channel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.concurrent.TimeUnit;

public class RaftRPCClient {
    private final RaftRPCServiceGrpc.RaftRPCServiceStub stub;
    private Raft raftObject;

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
                        if(value.getSuccess() == false) {
                            System.out.println("Converted to Follower state, Updated Current term to match the node who sent the append entry response");
                            raftObject.setCurrentState(Raft.NodeState.FOLLOWER);
                            raftObject.setCurrentTerm(value.getTerm());
                            raftObject.logger.info("(" + raftObject.getPORT() + "): Converted to Follower state. Updated Term to = " + value.getTerm());
                            raftObject.cancelAppendEntriesAll();
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

    public void requestVote(RequestVoteRequest request) {
        try {
            stub.withDeadlineAfter(this.raftObject.getElectionTimeout(), TimeUnit.SECONDS).requestVote(request, new StreamObserver<RequestVoteResponse>() {
                @Override
                public void onNext(RequestVoteResponse value) {
                    System.out.println("Received requestVote Response from " + value.getSenderPort() + "(term= " + value.getTerm() + ")" + " vote: " + value.getVoteGranted());

                    // Candidate received a valid append entry from a new leader and moved to FOLLOWER state
                    // Ignore the rest of the requestVote responses
                    if(raftObject.getCurrentState() == Raft.NodeState.FOLLOWER) {
                        return;
                    }

                    // Add logic to check if the votes received are majority
                    if(value.getVoteGranted()) {
                        raftObject.setVotesReceived(raftObject.getVotesReceived() + 1);
                    }

                    raftObject.logger.info("(" + raftObject.getPORT() + "): Received RequestVote Response from " + value.getSenderPort() + "(term= " + value.getTerm() + ")" + " vote: " + value.getVoteGranted() + " Total Votes = " + raftObject.getVotesReceived());

                    synchronized (raftObject) {
                        if(raftObject.getCurrentState() != Raft.NodeState.LEADER && raftObject.getVotesReceived() > raftObject.getServerCount() / 2) {
                            // Declare current server the leader
//                            if(raftObject.getCurrentState() !=  Raft.NodeState.LEADER) {
                                System.out.println("ELECTED AS LEADER! current term = " + raftObject.getCurrentTerm());
                                raftObject.logger.info("(" + raftObject.getPORT() + ") : Elected As LEADER!");
//                            }

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

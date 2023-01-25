package io.rahulpriolkar.raft;

import io.grpc.stub.StreamObserver;

public class RaftRPCService extends RaftRPCServiceGrpc.RaftRPCServiceImplBase {
    private Raft raftObj;
    
    public RaftRPCService(Raft raft) {
        this.raftObj = raft;
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        AppendEntriesResponse response = null;
        // implement logic (updating log etc)

        // the receiving node could be a FOLLOWER, or might ha ve already
        // timed out, moved to CANDIDATE and started an election

        // But appendEntriesRPC will always come from the leader (Actual leader or a node which believes it is a leader
        // eg: a previous leader which has come back online)

        // Case 1: (Current Node = FOLLOWER)
        // => Just discard the entry if the sender node's term is old
        //    Else just continue, you get a heartbeat from a leader

        // Case 2: (Current Node = CANDIDATE)
        // => Discard the entry if the sender node's term is old
        //    Else if its valid then cancel the election, and move to follower state
        // Should add logic to ignore responses to already sent request Votes ?

        // Must check term to validate if the sending node is LEADER. (Add Logic)
        if(request.getTerm() < this.raftObj.getCurrentTerm()) {
            raftObj.logger.info("(" + raftObj.getPORT() + ") : Received Append Entry - [Rejected] Sender(" + request.getSenderPort() + ") Term = " + request.getTerm() + " Current Term = " + raftObj.getCurrentTerm());
            response = AppendEntriesResponse.newBuilder()
                    .setTerm(this.raftObj.getCurrentTerm())
                    .setSuccess(false)
                    .build();
        } else {
            System.out.println("Cancelling Election! Received Append Entry from " + request.getSenderPort() + "(" + request.getTerm() + ")!");
            raftObj.logger.info("(" + raftObj.getPORT() + ") : Received Append Entry - [Accepted] Sender(" + request.getSenderPort() + ")  Term = " + request.getTerm() + " Current Term = " + raftObj.getCurrentTerm());
            response = AppendEntriesResponse.newBuilder()
                    .setTerm(this.raftObj.getCurrentTerm())
                    .setSuccess(true)
                    .build();

            // This is wrong, this is a receiver
            if(this.raftObj.getCurrentState() == Raft.NodeState.LEADER) {
                // cancel the previous heartbeats as a new leader with a higher term has been detected
                this.raftObj.cancelAppendEntriesAll();
            }

            // Moving to Follower state
            this.raftObj.setCurrentState(Raft.NodeState.FOLLOWER);
            this.raftObj.heartbeat(true); // should this heartbeat be called after onNext ?
        }

        responseObserver.onNext(response);
//        responseObserver.onCompleted();
    }

    @Override
    public void requestVote(RequestVoteRequest request, StreamObserver<RequestVoteResponse> responseObserver) {
        RequestVoteResponse response;

        // Only if the vote is granted to the other node requesting // actually shouldn't cancel election here
        // Why invoke heartbeat if a vote is granted ?? As the Candidate to whom the vote is granted might not
        // become the leader. // hence it makes sense to cancel an election only if an appendEntry is received.
//        this.raftObj.heartbeat();
        // But turns out you have to increment term, set hasVotedInCurrentTerm to true and call heartbeat
        // after granting a vote

        // Add logic to grant vote iff vote is not yet granted in current term and if the term of candidate >= current term of the node
        boolean heartbeatFlag = false;
        synchronized (raftObj) {
            if(request.getTerm() < raftObj.getCurrentTerm() || raftObj.getHasVotedInCurrentTerm() == true) {
                response = RequestVoteResponse.newBuilder()
                        .setTerm(raftObj.getCurrentTerm())
                        .setSenderPort(raftObj.getPORT())
                        .setVoteGranted(false)
                        .build();
            } else {
                raftObj.setCurrentTerm(raftObj.getCurrentTerm()+1);
                raftObj.setHasVotedInCurrentTerm(true);
                response = RequestVoteResponse.newBuilder()
                        .setTerm(raftObj.getCurrentTerm())
                        .setSenderPort(raftObj.getPORT())
                        .setVoteGranted(true)
                        .build();

                // Reset Election timeout (Call heartbeat) as a FOLLOWER / CANDIDATE ?
                // Only if the node is a FOLLOWER ?
                if(raftObj.getCurrentState() == Raft.NodeState.FOLLOWER) {
                    heartbeatFlag = true;
                }
            }
        }


        responseObserver.onNext(response);
        responseObserver.onCompleted();

        if(heartbeatFlag) {
            raftObj.heartbeat(true);
        }
    }
}

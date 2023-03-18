package io.rahulpriolkar.raft;

import io.grpc.stub.StreamObserver;

import java.util.ArrayList;

public class RaftRPCService extends RaftRPCServiceGrpc.RaftRPCServiceImplBase {
    private final Raft raftObj;
    
    public RaftRPCService(Raft raft) {
        this.raftObj = raft;
    }

    @Override
    public void appendEntries(AppendEntriesRequest request, StreamObserver<AppendEntriesResponse> responseObserver) {
        AppendEntriesResponse response;
        System.out.println("Before Processing Append Entry");
        // the receiving node could be a FOLLOWER, or might have already
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

       synchronized (raftObj) {
            // if receiver has higher term than the Leader
            boolean condition1 = request.getTerm() < this.raftObj.getCurrentTerm();

            // if the receiver's log is not large enough to contain prevLogIndex
            boolean condition2 = request.getPrevLogIndex() > -1
                    && request.getPrevLogIndex() >= raftObj.getLog().size();

            // if the entry in the receiver's log, at prevLogIndex has term != prevLogTerm
            boolean condition3 = request.getPrevLogIndex() > -1
                    && raftObj.getLog().size() > request.getPrevLogIndex() // check if index is out of bounds
                    && raftObj.getLog().get(request.getPrevLogIndex()).getTerm() != request.getPrevLogTerm();

            // Must check term to validate if the sending node is LEADER. (Add Logic)
            if(condition1 || condition2 || condition3) {
                if(condition1) System.out.println("Condition 1");
                if(condition2) System.out.println("Condition 2");
                if(condition3) System.out.println("Condition 3");
                System.out.println("Request.PrevLogIndex = " + request.getPrevLogIndex() + " RaftObj.LogSize = " + raftObj.getLog().size());
                raftObj.logger.info("(" + raftObj.getPORT() + ") : Received Append Entry - [Rejected] Sender(" + request.getSenderPort() + ") Term = " + request.getTerm() + " Current Term = " + raftObj.getCurrentTerm());
                response = AppendEntriesResponse.newBuilder()
                        .setTerm(this.raftObj.getCurrentTerm())
                        .setSuccess(false)
                        .setSenderPort(raftObj.getPORT())
                        .build();

                // Must call heartbeat here (Leads to duplicate appendEntriesAll call, everytime an appendEntryRequest
                // is received from a past LEADER that just came back online)
                // => Quick Fix: Call heartbeat on a failure iff current Node is not a leader
                // This happens because the appendEntriesAll (Leader Heartbeat is not periodic, the heartbeat is called just once,
                // and it runs a fixedRate scheduled future)
                // OR an appendEntriesCancel can be called on in the leader's heartbeat implementation, at the beginning
                if(raftObj.getCurrentState() != Raft.NodeState.LEADER)
                    this.raftObj.heartbeat(true); // should this heartbeat be called after onNext ?

            } else { // This else is reached only if the prevLogTerm at prevLogIndex matches or if prevLogIndex = -1
                System.out.println("Request.PrevLogIndex = " + request.getPrevLogIndex() + " RaftObj.LogSize = " + raftObj.getLog().size());

                // add logic to delete existing log entries after prevLogIndex
                // (Should this be done in the previous else-if, as we find each mismatch ??)
                // and append the Leader's log entries
                ArrayList<LogEntry> log = raftObj.getLog();

                // remove entries after prevLogIndex, if they exist
                if(raftObj.getLastLogIndex() > request.getPrevLogIndex()) { // is this if condition necessary ? - Yes
                    try {
                        log = new ArrayList<LogEntry>(log.subList(request.getPrevLogIndex() + 1, log.size()));
                    } catch(Exception e) {
                        System.err.println("Error while sub listing the log");
                        e.printStackTrace();
                    }
                }

                 log.addAll(request.getEntriesList());
                 raftObj.setLog(log);

                if(request.getCommitIndex() > raftObj.getCommitIndex()) {
                    raftObj.setCommitIndex(Math.min(request.getCommitIndex(), raftObj.getLastLogIndex()));
                }

                System.out.println("Cancelling Election! Received Append Entry from " + request.getSenderPort() + "(" + request.getTerm() + ")!");
                raftObj.logger.info("(" + raftObj.getPORT() + ") : Received Append Entry - [Accepted] Sender(" + request.getSenderPort() + ")  Term = " + request.getTerm() + " Current Term = " + raftObj.getCurrentTerm());
                response = AppendEntriesResponse.newBuilder()
                        .setTerm(this.raftObj.getCurrentTerm())
                        .setSuccess(true)
                        .setSenderPort(raftObj.getPORT())
                        .build();

                // This is wrong, this is a receiver ?
                if(this.raftObj.getCurrentState() == Raft.NodeState.LEADER) {
                    // cancel the previous heartbeats as a new leader with a higher term has been detected
                    this.raftObj.cancelAppendEntriesAll();
                }

                // Update currentTerm to match Leader's term and convert node to Follower state
                if(raftObj.getCurrentTerm() < request.getTerm()) {
                    raftObj.setCurrentTerm(request.getTerm());
                }
                this.raftObj.setCurrentState(Raft.NodeState.FOLLOWER);
                this.raftObj.heartbeat(true); // should this heartbeat be called after onNext ?
            }
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
            boolean raftSafetyCheck; // true if the requesting node's log is at least as up-to-date as this node
            if(request.getLastLogTerm() > raftObj.getLastLogTerm())
                raftSafetyCheck = true;
            else if (request.getLastLogTerm() == raftObj.getLastLogTerm()) {
                raftSafetyCheck = request.getLastLogIndex() >= raftObj.getLastLogIndex();
            }
            else {
                raftSafetyCheck = false;
            }

//            System.out.println("Raft Safety Check: " + raftSafetyCheck);
            if(raftSafetyCheck && request.getTerm() >= raftObj.getCurrentTerm() && !raftObj.getHasVotedInCurrentTerm()) {

//                raftObj.setCurrentTerm(raftObj.getCurrentTerm()+1);

                // Check if this is correct
                // if vote granted to a higher term candidate, then update currentTerm to match the Candidate's term
                if(request.getTerm() > raftObj.getCurrentTerm()) {
                    raftObj.setCurrentTerm(request.getTerm());
//                    raftObj.setCurrentState(Raft.NodeState.FOLLOWER);
                }

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
            } else {
                response = RequestVoteResponse.newBuilder()
                        .setTerm(raftObj.getCurrentTerm())
                        .setSenderPort(raftObj.getPORT())
                        .setVoteGranted(false)
                        .build();
            }
        }

        responseObserver.onNext(response);
        responseObserver.onCompleted();

        if(heartbeatFlag) {
            raftObj.heartbeat(true);
        }
    }
}

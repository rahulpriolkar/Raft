package io.rahulpriolkar.raft;

import io.grpc.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.*;

public class Raft {
    public Logger logger = LogManager.getLogger(Raft.class.getName());

    // all nodes (persistent)
    private long currentTerm = 0;
    public long getCurrentTerm() {
        return this.currentTerm;
    }
    public void setCurrentTerm(long term) {
        this.currentTerm = term;
    }

    private String candidateId;
    private ArrayList<LogEntry> log = new ArrayList<>();
    public ArrayList<LogEntry> getLog() {
        return this.log;
    }
    public void setLog(ArrayList<LogEntry> newLog) {
        this.log = newLog;
    }

    public int getLastLogIndex() {
        return this.log.size()-1;
    }
    public long getLastLogTerm() {
        return getLastLogIndex() >= 0 ? this.log.get(getLastLogIndex()).getTerm() : -1;
    }

    // all nodes (volatile)
    private int commitIndex = -1;
    private int lastApplied = -1;

    // leader
    private HashMap<Integer, Integer> nextIndex = new HashMap<>();
    private HashMap<Integer, Integer> matchIndex = new HashMap<>();

    public HashMap<Integer, Integer> getNextIndex() {
        return nextIndex;
    }
    public HashMap<Integer, Integer> getMatchIndex() {
        return matchIndex;
    }
    public void setNextIndex(HashMap<Integer, Integer> newNextIndex) {
        this.nextIndex = newNextIndex;
    }
    public void setMatchIndex(HashMap<Integer, Integer> newMatchIndex) {
        this.matchIndex = newMatchIndex;
    }

    private void initNextIndex() {
        for(int i = 0; i < this.neighbors.length; i++) {
            this.nextIndex.put(this.neighbors[i], this.getLastLogIndex()+1);
        }
    }
    private void initMatchIndex() {
        for(int i = 0; i < this.neighbors.length; i++) {
            this.nextIndex.put(this.neighbors[i], 0);
        }
    }

    private Integer getPrevLogIndex(Integer id) {
        return this.nextIndex.get(id) - 1;
    }
    private long getPrevLogTerm(Integer id) {
        return getPrevLogIndex(id) >= 0 ? this.log.get(getPrevLogIndex(id)).getTerm() : -1;
    }

    private int votesReceived = 0;
    public int getVotesReceived() {
        return this.votesReceived;
    }
    public void setVotesReceived(int votesReceived) {
        this.votesReceived = votesReceived;
    }

    // WHETHER THE NODE HAS VOTED IN THE CURRENT TERM
    private Boolean hasVotedInCurrentTerm = false;
    public Boolean getHasVotedInCurrentTerm() {
        return this.hasVotedInCurrentTerm;
    }
    public void setHasVotedInCurrentTerm(boolean val) {
        this.hasVotedInCurrentTerm = val;
    }

    // PORT
    private int PORT;
    public int getPORT() {
        return PORT;
    }

    // ELECTION TIMEOUT
    Random generator = new Random();
    private long electionTimeout = 5;
    public long getElectionTimeout() {
        return electionTimeout;
    }

    // SERVER COUNT
    // TODO
    // Should be able to change dynamically if more servers are added // IMPORTANT (Add Functionality)
    private int serverCount = 5;
    public int getServerCount() {
        return this.serverCount;
    }


    // NODE STATES (LEADER, CANDIDATE, FOLLOWER)
    enum NodeState {
        LEADER,
        CANDIDATE,
        FOLLOWER
    }
    private NodeState currentState;
    public NodeState getCurrentState() {
        return this.currentState;
    }
    public void setCurrentState(NodeState state) {
        // Initialize NextIndex and MatchIndex
        // This is set on starting the node or when the node wins the election
        if(state == NodeState.LEADER) {
            initNextIndex();
            initMatchIndex();
        }
        this.currentState = state;
    }

    // stores just the PORTs of the neighbors
    private int[] neighbors = null;

    RaftRPCService raftRPCService = new RaftRPCService(this);

    HashMap<Integer, RaftRPCClient> raftClients = new HashMap<Integer, RaftRPCClient>();
    private RaftRPCClient createRaftClient(int targetPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", targetPort)
                .usePlaintext()
                .build();
        return new RaftRPCClient(channel, this);
    }

    // EXECUTOR SERVICE - RUNS THE RAFT SERVER, APPEND ENTRIES, INITIATE ELECTION TASKS
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(3);
    public ScheduledExecutorService getExecutorService() {
        return executorService;
    }

    private ScheduledFuture currentElectionFuture = null;
    private ScheduledFuture nextElectionFuture = null;
    private initiateElection currentElectionRunnableObj = null;
    private initiateElection nextElectionRunnableObj = null;

    private appendEntriesAll appendEntriesAllRunnableObj = null;
    private Future appendEntriesAllFuture = null;

    Server raftServer = null;
    private startRaftServer startRaftServerRunnableObj = null;
    private Future startRaftServerFuture = null;


    public Raft(NodeState state, int port, int[] neighbors) throws InterruptedException, IOException {
        this.PORT = port;
        this.neighbors = neighbors;
        this.setCurrentState(state);

        System.out.println("Election Timeout = " + this.electionTimeout);

        // start future in a thread
        startRaftServerRunnableObj = new startRaftServer();
        startRaftServerFuture = executorService.submit(startRaftServerRunnableObj);

        // creating a raft client with all other servers, for dedicated communication
        for (int neighbor : this.neighbors) {
            this.raftClients.put(neighbor, this.createRaftClient(neighbor));
        }

        try {
            heartbeat(false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private class startRaftServer implements Runnable {

        @Override
        public void run() {
            try {
                raftServer = ServerBuilder.forPort(PORT)
                        .addService(raftRPCService)
                        .build()
                        .start();
                raftServer.awaitTermination();
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class CallRequestVote implements Runnable {
        private final RaftRPCClient client;
        CallRequestVote(RaftRPCClient client) {
            this.client = client;
        }

        @Override
        public void run() {
            RequestVoteRequest request = RequestVoteRequest.newBuilder().setCandidateId("0").setTerm(currentTerm).setLastLogTerm(0).setLastLogIndex(0).build();
            this.client.requestVote(request);
        }
    }

    private class initiateElection implements Runnable {
        ScheduledExecutorService electionExecutorService = Executors.newScheduledThreadPool(10);

        public void cancelElection() {
            electionExecutorService.shutdown(); // inspect .shutdown method in depth
        }

        @Override
        public void run() {
            // Change Term Related Variables
            currentTerm++;
            currentState = NodeState.CANDIDATE;
            votesReceived = 1;
            hasVotedInCurrentTerm = false;

            System.out.println("Running Initiate Election");
            System.out.println("term = " + currentTerm);

//            this.cancelElection(); // for split votes, the previous election also needs to be cancelled, but after
            // electionTimeout has passed (so cant be done in heartbeat, when the next initiateElection is scheduled)
            heartbeat(false); // call heartbeat to initiate new election if no result to the below requestVote RPCs after

            // TODO
            // requestVote RPC in parallel to all available servers
            // wait for majority response
            for(int key: raftClients.keySet()) {
                System.out.println("Requesting Vote");
                RequestVoteRequest request = RequestVoteRequest.newBuilder()
                        .setCandidateId("0")
                        .setTerm(currentTerm)
                        .setLastLogTerm(getLastLogTerm())
                        .setLastLogIndex(getLastLogIndex())
                        .build();
                raftClients.get(key).requestVote(request);

                // what's the difference between the above and below code ??
//                electionExecutorService.schedule(new CallRequestVote(client), 0, TimeUnit.SECONDS);
            }
        }
    }

    private class appendEntriesAll implements Runnable {
        @Override
        public void run() {
            System.out.println("Sending Append Entries heartbeat");
            // send appendEntries RPC in (parallel ??) to all available servers

            // check if each api call is blocking (IMPORTANT)
            for(int key: raftClients.keySet()) {
                AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                        .setTerm(currentTerm)
                        .setPrevLogIndex(getPrevLogIndex(key))
                        .setPrevLogTerm(getPrevLogTerm(key))
                        .addAllEntries(log)
                        .setCommitIndex(commitIndex)
                        .setSenderPort(getPORT())
                        .build();
                raftClients.get(key).appendEntries(request);
            }
        }
    }

    public void cancelAppendEntriesAll() {
        // check how cancel works if the runnable task is already being executed
        if(this.appendEntriesAllFuture != null && !this.appendEntriesAllFuture.isCancelled()) {
            this.appendEntriesAllFuture.cancel(true);
        }
        this.appendEntriesAllFuture = null;
    }


    public void heartbeat(Boolean cancelPreviousElection) {

        switch(this.currentState) {

            case LEADER:
                // Is this required ? Probably when an election was started and the current node
                // became the leader from a candidate ??
                this.currentElectionFuture = this.nextElectionFuture;
                this.currentElectionRunnableObj = this.nextElectionRunnableObj;

                if(this.currentElectionFuture != null && !this.currentElectionFuture.isCancelled()) {
                    // cancels an ongoing election, only if an appendEntries RPC is received
                    if(cancelPreviousElection)
                        this.currentElectionRunnableObj.cancelElection();

                    this.currentElectionFuture.cancel(false); // cancels scheduled future
                    System.out.println("Cancelled Election");
                }

                System.out.println("Starting append entries");
                // This schedule is not getting cancelled ? (think it did)
                this.appendEntriesAllFuture =  executorService.scheduleAtFixedRate(new appendEntriesAll(), 0, 1, TimeUnit.SECONDS);
                break;

            default: // FOLLOWER or CANDIDATE
                System.out.println("Running Heartbeat");

                // implement randomized timeouts

                // Cancel the election if appendEntries (cancelled from grpc appendEntries service file) is received
                // or in case of a split vote (cancelled from initiate election)
                // election is cancelled from
                // before starting a new one after "electionTimeout" time has passed
                this.currentElectionFuture = this.nextElectionFuture;
                this.currentElectionRunnableObj = this.nextElectionRunnableObj;

                if(this.currentElectionFuture != null && !this.currentElectionFuture.isCancelled()) {
                    // cancels an ongoing election, only if an appendEntries RPC is received
                    if(cancelPreviousElection)
                        this.currentElectionRunnableObj.cancelElection(); // cancels election which has already begun ??

                    // in-progress current election task is allowed to complete since mayInterruptRunning is set to false
                    this.currentElectionFuture.cancel(false); // cancels scheduled future, when node is follower
                    // and has scheduled an election
                    System.out.println("Cancelled Election");
                }

                // does this reassignment destroy the existing executor service in the previous object ??
                // I think it does!!
//                this.initiateElectionRunnableObj = new initiateElection();
//                this.initiateElectionFuture =
//                        executorService.schedule(initiateElectionRunnableObj, 8, TimeUnit.SECONDS);

                // Randomized Election Timeouts
                long currentElectionTimeout = this.electionTimeout*1000 + 150 + generator.nextInt(150);

                this.nextElectionRunnableObj = new initiateElection();
                this.nextElectionFuture = executorService.schedule(this.nextElectionRunnableObj, currentElectionTimeout, TimeUnit.MILLISECONDS);

                break;
        }
    }
}

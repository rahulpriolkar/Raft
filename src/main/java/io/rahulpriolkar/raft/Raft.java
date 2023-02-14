package io.rahulpriolkar.raft;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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
    private ArrayList<LogEntry> log = new ArrayList<LogEntry>();
    public int getLastLogIndex() {
        return this.log.size()-1;
    }
    public long getLastLogTerm() {
        return getLastLogIndex() >= 0 ? this.log.get(getLastLogIndex()).getTerm() : -1;
    }

    // all nodes (volatile)
    private long commitIndex;
    private long lastApplied;

    // leader
    private long[] nextIndex;
    private long[] matchIndex;

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
    private long electionTimeout = 12;
    public long getElectionTimeout() {
        return electionTimeout;
    }

    // SERVER COUNT
    // Should be able to change dynamically if more servers are added // IMPORTANT
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
        this.currentState = state;
    }

    private int[] neighbors = null;

    RaftRPCService raftRPCService = new RaftRPCService(this);

    List<RaftRPCClient> raftClients = new ArrayList<RaftRPCClient>();
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
        this.currentState = state;
        this.neighbors = neighbors;

        System.out.println("Election Timeout = " + this.electionTimeout);

        // start future in a thread
        startRaftServerRunnableObj = new startRaftServer();
        startRaftServerFuture = executorService.submit(startRaftServerRunnableObj);

        // creating a raft client with all other servers, for dedicated communication
        for(int i = 0; i < this.neighbors.length; i++) {
            this.raftClients.add(this.createRaftClient(this.neighbors[i]));
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
        private RaftRPCClient client;
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
            // "electionTimeout" has passed


            // TODO
            // requestVote RPC in parallel to all available servers
            // wait for majority response
            raftClients.forEach((client) -> {
                System.out.println("Requesting Vote");
                RequestVoteRequest request = RequestVoteRequest.newBuilder()
                        .setCandidateId("0")
                        .setTerm(currentTerm)
                        .setLastLogTerm(getLastLogTerm())
                        .setLastLogIndex(getLastLogIndex())
                        .build();
                client.requestVote(request);

                // what's the difference between the above and below code ??
//                electionExecutorService.schedule(new CallRequestVote(client), 0, TimeUnit.SECONDS);
            });
        }
    }

    private class appendEntriesAll implements Runnable {
        @Override
        public void run() {
            System.out.println("Sending Append Entries heartbeat");
            // send appendEntries RPC in (parallel ??) to all available servers
            AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                                                .setTerm(currentTerm)
                                                .setSenderPort(getPORT())
                                                .build();
            // check if each api call is blocking (IMPORTANT)
            raftClients.forEach((client) -> {
                client.appendEntries(request);
            });
        }
    }

    public void cancelAppendEntriesAll() {
        // check how cancel works if the runnable task is already being executed
        if(this.appendEntriesAllFuture != null && this.appendEntriesAllFuture.isCancelled() == false) {
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

                if(this.currentElectionFuture != null && this.currentElectionFuture.isCancelled() == false) {
                    // cancels an ongoing election, only if an appendEntries RPC is received
                    if(cancelPreviousElection)
                        this.currentElectionRunnableObj.cancelElection();

                    this.currentElectionFuture.cancel(false); // cancels scheduled future
                    System.out.println("Cancelled Election");
                }

                System.out.println("Starting append entries");
                // This schedule is not getting cancelled ? (think it did)
                this.appendEntriesAllFuture =  executorService.scheduleAtFixedRate(new appendEntriesAll(), 0, 5, TimeUnit.SECONDS);
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

                if(this.currentElectionFuture != null && this.currentElectionFuture.isCancelled() == false) {
                    // cancels an ongoing election, only if an appendEntries RPC is received
                    if(cancelPreviousElection)
                        this.currentElectionRunnableObj.cancelElection(); // cancels election which has already begun ??

                    // in-progress current election task is allowed to complete since mayInterruptRunning is set to false
                    this.currentElectionFuture.cancel(false); // cancels scheduled future, when node is follower
                    // and has scheduled an election
                    System.out.println("Cancelled Election");
                }

                // start an election, after electionTimeout has passed
                // schedules new future

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

    public static void main(String[] args) throws IOException, InterruptedException {
//        String timeStamp = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new java.util.Date());
//        final int PORT = 5001;
//        System.out.println("Hello");
//        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", PORT)
//                .usePlaintext()
//                .build();
//        RaftRPCClient raftRPCClient = new RaftRPCClient(channel);
//        Raft raftObj = new Raft(NodeState.FOLLOWER, PORT);
//        RaftRPCService raftRPCService = new RaftRPCService(raftObj);
//        Server raftRPCServer = ServerBuilder.forPort(PORT)
//                .addService(raftRPCService)
//                .build()
//                .start();
//        raftRPCServer.awaitTermination();

    }
}

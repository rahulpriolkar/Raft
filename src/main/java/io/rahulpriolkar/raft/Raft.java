package io.rahulpriolkar.raft;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Raft {
    public Logger logger = LogManager.getLogger(Raft.class.getName());

    // Http Server
    private HashMap<Integer, HttpExchange> clientMappings = new HashMap<>();

    // State Machine
    private StateMachine stateMachine = new StateMachine();

    ReadWriteLock lock = new ReentrantReadWriteLock();


    // all nodes (persistent)
    private long currentTerm = 0;
    public long getCurrentTerm() {
        return this.currentTerm;
    }
    public void setCurrentTerm(long term) {
        this.currentTerm = term;
    }

    private String candidateId;

    LogEntry defaultLogEntry = LogEntry.newBuilder()
            .setTerm(-1)
            .setKey("default")
            .setValue("-1")
            .build();
    private ArrayList<LogEntry> log = new ArrayList<>(Collections.nCopies(0, defaultLogEntry));
    public ArrayList<LogEntry> getLog() {
        return this.log;
    }
    public void setLog(ArrayList<LogEntry> newLog) {
        this.log = newLog;
    }

    public void displayLog() {
        for(int i = 0; i <= getLastLogIndex(); i++) {
            System.out.print(log.get(i).getValue() + " ");
        }
        System.out.println();
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

    public int getCommitIndex() {
        return this.commitIndex;
    }
    public void setCommitIndex(int index) {
        this.commitIndex = index;
    }

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
        for (int neighbor : this.neighbors) {
            this.nextIndex.put(neighbor, this.getLastLogIndex() + 1);
        }
    }
    private void initMatchIndex() {
        for (int neighbor : this.neighbors) {
            this.matchIndex.put(neighbor, -1); // should be initialized to -1 or 0 ??
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
    private final int PORT;
    public int getPORT() {
        return PORT;
    }

    // ELECTION TIMEOUT
    Random generator = new Random();
    private final long electionTimeout = 30;
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
    private int[] neighbors;

    RaftRPCService raftRPCService = new RaftRPCService(this);

    HashMap<Integer, RaftRPCClient> raftClients = new HashMap<Integer, RaftRPCClient>();
    private RaftRPCClient createRaftClient(int targetPort) {
        ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost", targetPort)
                .usePlaintext()
                .build();
        return new RaftRPCClient(channel, this);
    }

    // EXECUTOR SERVICE - RUNS THE RAFT SERVER, APPEND ENTRIES, INITIATE ELECTION TASKS
    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
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
    private startRaftServer startRaftServerRunnableObj;
    private Future startRaftServerFuture;

    private startHttpServer startHttpServerRunnableObj;
    private Future startHttpServerFuture;


    public Raft(NodeState state, int port, int[] neighbors) throws InterruptedException, IOException {
        this.PORT = port;
        this.neighbors = neighbors;
        this.setCurrentState(state);

        System.out.println("Election Timeout = " + this.electionTimeout);

        // start future in a thread
        startRaftServerRunnableObj = new startRaftServer();
        startRaftServerFuture = executorService.submit(startRaftServerRunnableObj);

        startHttpServerRunnableObj = new startHttpServer();
        startHttpServerFuture = executorService.submit(startHttpServerRunnableObj);

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
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private class startHttpServer implements Runnable {
        @Override
        public void run() {
            HttpServer server;
            try {
                server = HttpServer.create(new InetSocketAddress(PORT+1000), 0);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            server.createContext("/test", new MyHandler());
            server.setExecutor(null); // creates a default executor
            server.start();
        }

        class MyHandler implements HttpHandler {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                logger.log(Level.valueOf("info"), "Handling HTTP Request");

                LogEntry logEntry = commandToLogEntry(exchange.getRequestBody());

                lock.writeLock().lock();
                // Add command to log
                System.out.println("Entered Critical Region");
                log.add(logEntry);
                // Save the OutputStream in the clientMappings (Respond with success/failure after committing) // Should happen from heartbeat
                clientMappings.put(getLastLogIndex(), exchange);
                lock.writeLock().unlock();
                System.out.println("Exited Critical Region");

//                exchange.sendResponseHeaders(200, res.length());
//                OutputStream os = exchange.getResponseBody();
//                os.write(res.getBytes());
//                os.close();
            }
        }

        private LogEntry commandToLogEntry(InputStream inputStream) throws IOException{
            ByteArrayOutputStream result = new ByteArrayOutputStream();
            byte[] buffer = new byte[1024];
            for (int length; (length = inputStream.read(buffer)) != -1; ) {
                result.write(buffer, 0, length);
            }

            // StandardCharsets.UTF_8.name() > JDK 7
            String command = result.toString("UTF-8");
            String[] keyVal = command.split("=");
            keyVal[0] = keyVal[0].trim();
            keyVal[1] = keyVal[1].trim();

            LogEntry logEntry = LogEntry.newBuilder()
                    .setTerm(currentTerm)
                    .setKey(keyVal[0])
                    .setValue(keyVal[1])
                    .build();

            return logEntry;
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

//                electionExecutorService.schedule(new CallRequestVote(client), 0, TimeUnit.SECONDS);
            }
        }
    }

    private class appendEntriesAll implements Runnable {
        @Override
        public void run() {
//            System.out.println("Sending Append Entries heartbeat");
            displayLog();
//            System.out.println("[After Display]: Sending Append Entries heartbeat");
            System.out.println("raftClients size = " + raftClients.size());

            // Update Commit Index
            this.updateCommitIndex();
            System.out.println("Commit Index = " + commitIndex);

            // send appendEntries RPC in (parallel ??) to all available servers (Update: They are sent asynchronously,
            // in one thread, not in parallel)

            // check if each api call is blocking (IMPORTANT) (Update: It's not)
            for(int key: raftClients.keySet()) {
                try {
                    System.out.println("NextIndex = " + nextIndex.get(key));
                    int prevLogIndex = getPrevLogIndex(key);
                    long prevLogTerm = getPrevLogTerm(key);
                    System.out.println("In Append Entries, before sending request - prevLogIndex = " + prevLogIndex + " prevLogTerm = " + prevLogTerm);

                    ArrayList<LogEntry> newEntries;
                    if(log.size() > 0) newEntries = new ArrayList<>(log.subList(prevLogIndex + 1, log.size()));
                    else newEntries = new ArrayList<>();

                    AppendEntriesRequest request = AppendEntriesRequest.newBuilder()
                            .setTerm(currentTerm)
                            .setPrevLogIndex(getPrevLogIndex(key))
                            .setPrevLogTerm(getPrevLogTerm(key))
                            .addAllEntries(newEntries)
                            .setCommitIndex(commitIndex)
                            .setSenderPort(getPORT())
                            .build();
                    raftClients.get(key).appendEntries(request);
                } catch (Exception e) {
                    System.out.println("Error: Append Entries");
                    e.printStackTrace();
                }
            }
        }

        // Test this function thoroughly
        private void updateCommitIndex() {
            // get the log index such that a majority of the matchIndex's are >= N and log term at index N == currentTerm
            // Get the list of match Index values, sort them in ascending order, and get the (serverCount / 2)th value
            try {
                ArrayList<Integer> matchIndexVals = new ArrayList<>(matchIndex.values());
//                System.out.println("Check: " + matchIndex.get(0));
                System.out.println("matchIndexVals = " + matchIndexVals);
                matchIndexVals.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        if (o1 < o2) return -1;
                        if (o1 > o2) return 1;
                        else return 0;
                    }
                });
                int newCommitIndex = matchIndexVals.get((serverCount-1) / 2);

                System.out.println("New Commit Index = " + newCommitIndex);

                // Decrement the index until the log term at that index has the same term as the leader's currentTerm
                // Can this go to -1 when a new leader is elected [???]
                while(newCommitIndex > -1 && log.get(newCommitIndex).getTerm() != currentTerm)
                    newCommitIndex--;
                commitIndex = newCommitIndex;
            } catch (Exception e) {
                e.printStackTrace();
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
                this.appendEntriesAllFuture =  executorService.scheduleAtFixedRate(new appendEntriesAll(), 0, 4, TimeUnit.SECONDS);
                break;

            default: // FOLLOWER or CANDIDATE
                System.out.println("Running Heartbeat");
                displayLog();

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

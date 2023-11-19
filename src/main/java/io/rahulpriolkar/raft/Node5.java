package io.rahulpriolkar.raft;

import java.io.IOException;

public class Node5 {
    public static void main(String[] args) throws InterruptedException, IOException {
        int[] raft5Neighbors = { 5001, 5002, 5003, 5004 };
        Raft raft5 = new Raft(Raft.NodeState.FOLLOWER, 5005, raft5Neighbors);
    }
}

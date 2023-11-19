package io.rahulpriolkar.raft;

import java.io.IOException;

public class Node4 {
    public static void main(String[] args) throws InterruptedException, IOException {
        int[] raft4Neighbors = { 5001, 5002, 5003, 5005 };
        Raft raft4 = new Raft(Raft.NodeState.FOLLOWER, 5004, raft4Neighbors);
    }
}

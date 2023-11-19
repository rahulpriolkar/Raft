package io.rahulpriolkar.raft;

import java.io.IOException;

public class Node3 {
    public static void main(String[] args) throws InterruptedException, IOException {
        int[] raft3Neighbors = { 5001, 5002, 5004, 5005 };
//        int[] raft3Neighbors = { 5001, 5002 };
        Raft raft3 = new Raft(Raft.NodeState.FOLLOWER, 5003, raft3Neighbors);
    }
}

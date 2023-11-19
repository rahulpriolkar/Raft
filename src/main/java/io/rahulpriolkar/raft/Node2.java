package io.rahulpriolkar.raft;

import java.io.IOException;

public class Node2 {
    public static void main(String[] args) throws InterruptedException, IOException {
        int[] raft2Neighbors = { 5001, 5003, 5004, 5005 };
//        int[] raft2Neighbors = { 5001, 5003 };
        Raft raft2 = new Raft(Raft.NodeState.FOLLOWER, 5002, raft2Neighbors);
    }
}
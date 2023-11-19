package io.rahulpriolkar.raft;

import java.io.IOException;

public class Node1 {
    public static void main(String[] args) throws InterruptedException, IOException {
        int[] raft1Neighbors = { 5002, 5003, 5004, 5005 };
//        int[] raft1Neighbors = { 5002, 5003 };
        Raft raft1 = new Raft(Raft.NodeState.LEADER, 5001, raft1Neighbors);
//        Thread.sleep(20*1000);
//        raft1.getExecutorService().shutdownNow();
//        System.exit(0);
//        ArrayList<Integer> arr = new ArrayList<>();
//        System.out.println(arr.get(0));
    }
}

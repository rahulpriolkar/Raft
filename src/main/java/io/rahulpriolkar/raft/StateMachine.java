package io.rahulpriolkar.raft;

import java.util.HashMap;

public class StateMachine {
    private HashMap<String, String> db = new HashMap<>();

    private String[] commandToKeyVal(String command) {
        String[] keyVal = command.split("=");
        keyVal[0] = keyVal[0].trim();
        keyVal[1] = keyVal[1].trim();
        return keyVal;
    }

    public void apply(String command) {
        String[] keyVal = commandToKeyVal(command);
        db.put(keyVal[0], keyVal[1]);
    }
}

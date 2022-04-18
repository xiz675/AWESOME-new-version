package edu.sdsc.queryprocessing.executor.execution.elementvariable;

import edu.sdsc.utils.Pair;

import java.util.HashMap;
import java.util.Map;

public class ExecutionVariableTable {
    private Map<Pair<Integer, String>, ExecutionTableEntry> variables;
    public ExecutionVariableTable() {
        variables = new HashMap<>();
    }

    public void insertEntry(Pair<Integer, String> key, ExecutionTableEntry ete){
        variables.put(key, ete);
    }

    public boolean hasTableEntry(Pair<Integer, String> key) {return variables.containsKey(key);}

    public ExecutionTableEntry getTableEntry(Pair<Integer, String> key) {
        return variables.get(key);
    }



}

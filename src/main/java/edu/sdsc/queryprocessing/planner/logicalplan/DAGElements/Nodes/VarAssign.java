package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import javax.json.JsonObject;
import java.util.HashSet;
import java.util.Set;

public class VarAssign extends Operator {
    public VarAssign(JsonObject var) {
        Set<Integer> input = new HashSet<>();
        if (var.containsKey("localVariable")) {
            this.addBlockUsed(var.getInt("blockID"));
        }
        else {
            input.add(var.getInt("varID"));
            this.setInputVar(input);
        }
    }
}

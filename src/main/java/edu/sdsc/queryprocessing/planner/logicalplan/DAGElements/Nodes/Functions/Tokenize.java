package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class Tokenize extends FunctionOperator {
    public Tokenize(JsonArray parameters) {
        super(parameters);
    }
}

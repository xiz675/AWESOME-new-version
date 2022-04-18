package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.FunctionOperator;

import javax.json.JsonArray;

public class Union extends FunctionOperator {
    public Union(JsonArray parameters) {
        super(parameters);
    }
}

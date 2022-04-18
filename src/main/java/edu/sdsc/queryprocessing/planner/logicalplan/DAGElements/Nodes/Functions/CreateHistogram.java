package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class CreateHistogram extends FunctionOperator {
    public CreateHistogram(JsonArray parameters) {
        super(parameters);
    }
}

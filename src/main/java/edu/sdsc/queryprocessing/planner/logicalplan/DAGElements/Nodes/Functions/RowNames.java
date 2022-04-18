package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class RowNames extends FunctionOperator {
    public RowNames(JsonArray parameters) {
        super(parameters);
    }
}

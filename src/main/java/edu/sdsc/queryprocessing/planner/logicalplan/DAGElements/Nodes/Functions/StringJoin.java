package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class StringJoin extends FunctionOperator{
    public StringJoin(JsonArray parameters) {
        super(parameters);
    }
}

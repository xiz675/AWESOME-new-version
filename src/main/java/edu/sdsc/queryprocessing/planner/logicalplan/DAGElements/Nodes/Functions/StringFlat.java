package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class StringFlat extends FunctionOperator{
    public StringFlat(JsonArray parameters) {
        super(parameters);
    }
}

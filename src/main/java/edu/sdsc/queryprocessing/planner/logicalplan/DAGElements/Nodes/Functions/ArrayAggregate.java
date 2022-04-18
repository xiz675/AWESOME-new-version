package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class ArrayAggregate extends FunctionOperator {
    public ArrayAggregate(JsonArray parameters) {
        super(parameters);
    }
}

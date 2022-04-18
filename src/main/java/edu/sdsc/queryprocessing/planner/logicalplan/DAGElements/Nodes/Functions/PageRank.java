package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class PageRank extends FunctionOperator {
    public PageRank(JsonArray parameters) {
        super(parameters);
    }
}
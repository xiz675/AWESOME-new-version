package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Operator;

import javax.json.JsonObject;
import java.util.*;

public class BuildGraphFromRelation extends Operator {
//    private boolean directed;
//    private String sourceLabel = null;
//    private String targetLabel = null;
//    private String edgeLabel = null;
//    private Object[] sourcePropertyWithValue;
//    private Object[] tgtPropertyWithValue;
//    private Object[] edgePropertyWithValue;


    private JsonObject constructionDetail;

    public BuildGraphFromRelation(JsonObject parserResult) {
        this.constructionDetail = parserResult.getJsonObject("RHS");
        Set<Integer> var = new HashSet<>();
        var.add(this.constructionDetail.getJsonObject("variable").getInt("varID"));
        this.setInputVar(var);
    }

    public JsonObject getConstructionDetail() {
        return constructionDetail;
    }
}

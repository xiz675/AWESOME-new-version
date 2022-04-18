package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators;

import javax.json.JsonObject;
import java.util.HashSet;
import java.util.Set;

public class ReduceExpression extends HighLevelOperator{
    private JsonObject reducedVar;
    private JsonObject leftLocalVar;
    private JsonObject rightLocalVar;
    private Set<Integer> reducedVarID = new HashSet<>();

    public ReduceExpression(JsonObject reducedVar, JsonObject leftLocalVar, JsonObject rightLocalVar) {
        this.reducedVar = reducedVar;
        this.leftLocalVar = leftLocalVar;
        this.rightLocalVar = rightLocalVar;
//
        this.reducedVarID.add(reducedVar.getInt("varID"));
        // if global, set it; else no
        if (!reducedVar.containsKey("localVariable")) {
            this.setInputVar(this.reducedVarID);
        }
        else {
            this.addBlockUsed(reducedVar.getInt("blockID"));
        }
    }
//    public MapExpression(JsonObject mappedVar, JsonObject iterratevar, Integer mapOpe, boolean returnOpe, Integer vID)
//    {
//        this.mappedVar = mappedVar;
//        this.iterratevar = iterratevar;
//        this.operation = mapOpe;
//        if (returnOpe) {
//            setReturnOperator(true);
//            setOutputOperator(vID);
//        }
////        // if global, set it; else no
////        this.mappedVarID.add(mappedVar.getInt("varID"));
////        this.setInputVar(this.mappedVarID);
//    }

    public Integer getReducedVarID() {
        return reducedVarID.iterator().next();
    }

//    public Integer getOperation() {
//        return operation;
//    }

    public JsonObject getReducedVar() {
        return reducedVar;
    }


    public JsonObject getLeftLocalVar() {
        return leftLocalVar;
    }

    public JsonObject getRightLocalVar() {
        return rightLocalVar;
    }

}

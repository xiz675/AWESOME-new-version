package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators;

import edu.sdsc.variables.logicalvariables.DataTypeEnum;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class MapExpression extends HighLevelOperator {
//    private Integer operation;
    private JsonObject mappedVar;
    private JsonObject iterateVar;
    private Set<Integer> mappedVarID = new HashSet<>();
    private List<Integer> localVarID = new ArrayList<>();
    private DataTypeEnum elementType = DataTypeEnum.Undecided;

    public MapExpression(JsonObject mappedVar, JsonObject iterateVar) {
        this.mappedVar = mappedVar;
        this.iterateVar = iterateVar;
        this.mappedVarID.add(mappedVar.getInt("varID"));
        this.localVarID.add(iterateVar.getInt("varID"));
        this.elementType = DataTypeEnum.valueOf(iterateVar.getString("varType"));
        // if global, set it; else no
        if (!mappedVar.containsKey("localVariable")) {
            this.setInputVar(this.mappedVarID);
        }
//        this.setInputOperator();
        else {
            this.addBlockUsed(mappedVar.getInt("blockID"));
        }
        // todo: set them
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

    public Integer getMappedVarID() {
        return mappedVarID.iterator().next();
    }

//    public Integer getOperation() {
//        return operation;
//    }

    public JsonObject getMappedVar() {
        return mappedVar;
    }

    public JsonObject getIterateVar() {
        return iterateVar;
    }

    public List<Integer> getLocalVarID() {
        return localVarID;
    }

    public DataTypeEnum getElementType() {
        return elementType;
    }
}

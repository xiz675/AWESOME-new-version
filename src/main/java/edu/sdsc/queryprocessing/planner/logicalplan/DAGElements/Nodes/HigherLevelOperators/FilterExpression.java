package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators;

import edu.sdsc.variables.logicalvariables.DataTypeEnum;

import javax.json.JsonObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

// a filter expression always accept a filtered variable (which can be a global or local variable) and an operator
// which has been evaluated

public class FilterExpression extends HighLevelOperator {
    private Integer filterOperator;
    private JsonObject filteredVar;
    private Set<Integer> filteredVarID = new HashSet<>();
    private JsonObject filterExpression;
    private List<Integer> localVarID = new ArrayList<>();
    private DataTypeEnum elementType = DataTypeEnum.Undecided;
    private DataTypeEnum appliedVarType = DataTypeEnum.Undecided;


    public FilterExpression(JsonObject filteredVar, JsonObject filterExpression, JsonObject iterateVar) {
        this.filteredVar = filteredVar;
        this.filterExpression = filterExpression;
        this.filteredVarID.add(filteredVar.getInt("varID"));
        this.localVarID.add(iterateVar.getInt("varID"));
        this.elementType = DataTypeEnum.valueOf(iterateVar.getString("varType"));
        this.appliedVarType = DataTypeEnum.valueOf(filteredVar.getString("varType"));
        // if global, set it; else no
        if (!filteredVar.containsKey("localVariable")) {
            this.setInputVar(this.filteredVarID);
        }
        else {
            this.addBlockUsed(filteredVar.getInt("blockID"));
        }
    }

    public void setFilterOperator(Integer filterOperator) {
        this.filterOperator = filterOperator;
    }

    public Integer getFilterOperator() {
        return filterOperator;
    }

    public JsonObject getFilteredVar() {
        return filteredVar;
    }

    public Integer getFilteredVarID() {
        return filteredVarID.iterator().next();
    }
    public List<Integer> getLocalVarID() {
        return localVarID;
    }

    public DataTypeEnum getElementType() {
        return elementType;
    }

    public void setAppliedVarType(DataTypeEnum appliedVarType) {
        this.appliedVarType = appliedVarType;
    }

    public DataTypeEnum getAppliedVarType() {
        return appliedVarType;
    }
}

package edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators;

import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.utils.Pair;

import javax.json.JsonArray;
import java.util.Map;

public abstract class DBPhysical extends PhysicalOperator {
    private boolean varDependent;
    private String statement;
    private Map<Integer, Pair<Integer, Integer>> varPosition;
    private JsonArray schema;

    public Map<Integer, Pair<Integer, Integer>> getVarPosition() {
        return varPosition;
    }

    public String getStatement() {
        return statement;
    }

    public boolean isVarDependent() {
        return varDependent;
    }

    public void setStatement(String statement) {
        this.statement = statement;
    }

    public void setVarDependent(boolean varDependent) {
        this.varDependent = varDependent;
    }

    public void setVarPosition(Map<Integer, Pair<Integer, Integer>>  varPosition) {
        this.varPosition = varPosition;
    }

    public void setSchema(JsonArray schema) {
        this.schema = schema;
    }

    public JsonArray getSchema() {
        return schema;
    }
}

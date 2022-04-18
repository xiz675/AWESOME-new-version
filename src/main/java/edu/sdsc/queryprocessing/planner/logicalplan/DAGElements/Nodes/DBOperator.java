package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import edu.sdsc.utils.Pair;

import javax.json.Json;
import javax.json.JsonArray;
import java.util.Map;

public abstract class DBOperator extends Operator {
    private boolean varDependent;
    private String statement;
    private Map<Integer, Pair<Integer, Integer>> varPosition;
    private String dbName = "*";
    private JsonArray schema = Json.createArrayBuilder().build();

    public void setSchema(JsonArray schema) {
        this.schema = schema;
    }

    public JsonArray getSchema() {
        return schema;
    }
    public void setDbName(String name) {this.dbName = name;}

    public String getDbName() {
        return dbName;
    }

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
}

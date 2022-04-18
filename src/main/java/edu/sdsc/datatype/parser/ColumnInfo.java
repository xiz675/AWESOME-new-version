package edu.sdsc.datatype.parser;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

public class ColumnInfo {
    private  String name;
    private  String tableSource = "postgres";
    private  String rawTableName;
    private  String rawColName;
    private  String objectType;
    private  boolean awsmVar = false;
    private  Integer varID = -1;
    private boolean isFunction = false;

    public ColumnInfo(String name, String rawTableName, String rawColName, String objectType) {
        this.name = name;
        this.rawTableName = rawTableName;
        this.objectType = objectType;
        this.rawColName = rawColName;
    }

    public void setFunction(boolean function) {
        this.isFunction = function;
    }

    public void setVarID(Integer awsmVar) {
        this.awsmVar = true;
        this.varID = awsmVar;
        this.tableSource = "awesome";
    }


    public JsonObject toJsonObject() {
        JsonObjectBuilder temp = Json.createObjectBuilder();
        temp.add("name", this.name).add("rawTableName", this.rawTableName)
                .add("rawColName",this.rawColName).add("objectType", this.objectType).add("tableSource", this.tableSource);
        if (this.varID > 0) {
            temp.add("varID", varID);
        }
        if (this.isFunction) {
            temp.add("isSQLFunction", "true");
        }
        return temp.build();
    }
}

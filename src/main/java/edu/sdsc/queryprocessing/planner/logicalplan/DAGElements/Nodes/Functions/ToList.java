package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;
import javax.json.JsonObject;

public class ToList extends FunctionOperator{
    Integer rID;
    String rName;
    String cName;

    public ToList(JsonArray parameters) {
        super(parameters);
        JsonObject temp = parameters.getJsonObject(0);
        String s = temp.getString("varName");
        String[] t = s.split("\\.");
        cName = t[1];
        rName = t[0];
        rID = temp.getInt("varID");
    }

    public String getrName() {
        return rName;
    }

    public Integer getrID() {
        return rID;
    }

    public String getcName() {
        return cName;
    }
}

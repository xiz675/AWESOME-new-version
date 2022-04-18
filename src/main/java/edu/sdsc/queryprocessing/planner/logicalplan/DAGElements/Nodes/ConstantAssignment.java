package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import edu.sdsc.utils.JsonUtil;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.HashSet;
import java.util.Set;

public class ConstantAssignment extends Operator {
    private Object value;
    public ConstantAssignment(JsonObject var) {
        String type = var.getString("type");
        if (type.equals("String")) {
            this.value = var.getString("value");
        }
        else if(type.equals("Double")) {
            this.value =  Double.parseDouble(var.getString("value"));
        }
        else {
            assert type.equals("Integer");
            this.value = var.getInt("value");
        }
    }

    public ConstantAssignment(JsonObject lhs, JsonObject rhs) {
        JsonArray v = rhs.getJsonArray("value");
        String eleType = lhs.getString("elementType");
        int id = lhs.getInt("varID");
        Set<Integer> produce = new HashSet<>();
        produce.add(id);
        setOutputVar(produce);
        // todo : add other types
        if (eleType.equals("String")) {
            this.value = JsonUtil.jsonArrayToStringList(v);
        }
        else {
            assert eleType.equals("Integer");
            this.value = JsonUtil.jsonArrayToIntList(v);
        }

//        String type = var.getString("type");
//        if (type.equals("String")) {
//            this.value = var.getString("value");
//        }
//        else if(type.equals("Double")) {
//            this.value =  Double.parseDouble(var.getString("value"));
//        }
//        else {
//            assert type.equals("Integer");
//            this.value = var.getInt("value");
//        }
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }
}

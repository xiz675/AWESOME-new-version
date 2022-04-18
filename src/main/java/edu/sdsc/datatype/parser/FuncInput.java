package edu.sdsc.datatype.parser;

import edu.sdsc.utils.JsonUtil;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;
import java.util.List;

public class FuncInput {
    public boolean variable = false;
    public Integer varID;
    public Integer block = -1;
    public String type = "Undecided";
    public String name = null;
    public Object value = null;
    public String key = null;
    // todo: unified with localVar
    // this is for where expression, can be set when parsing (should be unified with localVar)
    public boolean lambdaVar = false;
    public Integer iteratedVarID;
    // this is for map and reduce expressions
    public boolean localVar = false;
    public Integer parentVarID;


    public void setIteratedVarID(Integer iteratedVarID) {
        this.lambdaVar = true;
        this.iteratedVarID = iteratedVarID;
    }

    public Integer getIteratedVarID() {
        return this.iteratedVarID;
    }

    public void setParentVarID(Integer parentVarID) {
        this.localVar = true;
        this.parentVarID = parentVarID;
    }

    public void setValue(Object value, String type) {
        this.value = value;
        this.variable = false;
        this.type = type;
    }

    public JsonObject toJsonObject(){
        JsonObjectBuilder job = Json.createObjectBuilder();
        job.add("type", type);
        if (lambdaVar) {
            job.add("lambdaVariable", "true");
        }
        else if(localVar) {
            if (varID != null) {
                job.add("varID", varID);
            }
            job.add("localVariable", "true").add("blockID", block).add("parentID", parentVarID).add("varName", name);
        }
        else if (variable) {
            job.add("variable", "true").add("varName", name).add("varID", varID);
        }

        else {
            job.add("variable", "false");
            if (type.equals("Integer")) {
//                System.out.println(value);
                job.add("value", (Integer) value);
            }
            else if (type.equals("String")) {
                job.add("value", (String) value);
            }
            else if (type.equals("Boolean")) {
                job.add("value", (Boolean) value);
            }
            // todo: add List<Integer>
            else if (type.equals("List")) {
                job.add("value", JsonUtil.arrayToJsonArray((List<String>) value));
            }
        }
        if (key != null) {
            job.add("optionalVariable", "true").add("key", key);
        }
        return job.build();
    }

}

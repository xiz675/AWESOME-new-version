package edu.sdsc.datatype.execution;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.utils.Pair;

import javax.json.JsonArray;
import javax.json.JsonObject;

import java.util.List;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
import static edu.sdsc.utils.ParserUtil.jsonArrayToStringList;

public class FunctionParameter {
    private String key = null;
    private boolean hasValue = false;
    private boolean hasKey = false;
    private Object value = null;
    private boolean isVar = false;
    private Integer varID = null;

    public FunctionParameter(JsonObject js) {
        this.hasValue = js.containsKey("value");
        if (this.hasValue) {
            String type = js.getString("type");
            switch (type) {
                case "String":
                    this.value = js.getString("value");
                    break;
                case "Integer":
                    this.value = js.getInt("value");
                    break;
                case "Boolean":
                    this.value = js.getBoolean("value");
                    break;
                case "List":
                    JsonArray temp = js.getJsonArray("value");
                    // todo: add integer
                    this.value = jsonArrayToStringList(temp);
                    break;
            }
        }
        else {
            this.isVar = true;
            this.varID = js.getInt("varID");
        }
        if (js.containsKey("key")) {
            this.hasKey = true;
            this.key = js.getString("key");
        }
    }

    public boolean isHasKey() {
        return hasKey;
    }

    public boolean isHasValue() {
        return hasValue;
    }

    public String getKey() {
        return key;
    }

    public boolean isVar() {
        return isVar;
    }

    public Integer getVarID() {
        return varID;
    }

    public Object getValue() {
        return value;
    }

    public Object getValueWithExecutionResult(ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
        if (this.hasValue) {
            return value;
        }
        else {
            assert this.isVar;
            return getTableEntryWithLocal(new Pair<>(this.varID, "*"), evt, localEvt).getValue();
        }
    }

}

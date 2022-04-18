package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import javax.json.JsonArray;

public class BlackBoxFunc extends FunctionOperator {
    private String funcName;

    public BlackBoxFunc(String name, JsonArray parameters) {
        super(parameters);
        this.funcName = name;
    }

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }
}

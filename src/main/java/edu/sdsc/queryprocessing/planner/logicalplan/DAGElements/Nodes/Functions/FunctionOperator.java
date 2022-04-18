package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Operator;

import javax.json.Json;
import javax.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

public abstract class FunctionOperator extends Operator
{
    private List<Integer> outputWithOrder = new ArrayList<>();

    private JsonArray parameters = Json.createArrayBuilder().build();

    public FunctionOperator(JsonArray parameters) {
        this.setParameters(parameters);
    }

    public JsonArray getParameters() {
        return parameters;
    }

    public void setParameters(JsonArray parameters) {
        this.parameters = parameters;
    }

    public void setOutputWithOrder(List<Integer> outputWithOrder) {
        this.outputWithOrder = outputWithOrder;
    }

    public List<Integer> getOutputWithOrder() {
        return outputWithOrder;
    }
}

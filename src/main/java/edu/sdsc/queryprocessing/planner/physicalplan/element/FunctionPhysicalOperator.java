package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.utils.Pair;

import javax.json.Json;
import javax.json.JsonArray;
import java.util.ArrayList;
import java.util.List;

public abstract class FunctionPhysicalOperator extends PhysicalOperator{
    private JsonArray parameters = Json.createArrayBuilder().build();
    private List<Pair<Integer, String>> outputWithOrder = new ArrayList<>();

    public void setOutputWithOrder(List<Pair<Integer, String>> outputWithOrder) {
        this.outputWithOrder = outputWithOrder;
    }

    public List<Pair<Integer, String>> getOutputWithOrder() {
        return outputWithOrder;
    }

    public JsonArray getParameters() {
        return parameters;
    }

    public void setParameters(JsonArray parameters) {
        this.parameters = parameters;
    }
}

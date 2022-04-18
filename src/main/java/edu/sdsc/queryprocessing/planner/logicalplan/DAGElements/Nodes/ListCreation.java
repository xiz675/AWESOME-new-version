package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import javax.json.JsonObject;

public class ListCreation extends Operator {
    private JsonObject start;
    private JsonObject end;
    private JsonObject step;


    public ListCreation(JsonObject s, JsonObject e, JsonObject step) {
        this.start = s;
        this.end = e;
        this.step = step;
    }

    public JsonObject getStart() {
        return start;
    }

    public JsonObject getEnd() {
        return end;
    }

    public JsonObject getStep() {
        return step;
    }
}

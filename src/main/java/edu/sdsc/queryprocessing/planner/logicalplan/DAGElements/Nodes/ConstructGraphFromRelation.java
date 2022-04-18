package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

import javax.json.JsonObject;

public class ConstructGraphFromRelation extends Operator{
    Integer relationUsed;
    String relationName;
    boolean hasDirection;
    JsonObject source;
    JsonObject target;
    JsonObject edge;

    public ConstructGraphFromRelation(Integer vID, String rName, boolean direction, JsonObject source, JsonObject target, JsonObject edge) {
        this.relationUsed = vID;
        this.relationName = rName;
        this.hasDirection = direction;
        this.source = source;
        this.target = target;
        this.edge = edge;
    }

    public boolean isHasDirection() {
        return hasDirection;
    }

    public String getRelationName() {
        return relationName;
    }

    public Integer getRelationUsed() {
        return relationUsed;
    }

    public JsonObject getSource() {
        return source;
    }

    public JsonObject getEdge() {
        return edge;
    }

    public JsonObject getTarget() {
        return target;
    }
}

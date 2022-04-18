package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;

public class ExecuteCypher extends DBOperator {
    private Integer graphID = null;



    public void setGraphID(Integer id) {
        this.graphID = id;
    }

    public Integer getGraphID() {
        return graphID;
    }
}

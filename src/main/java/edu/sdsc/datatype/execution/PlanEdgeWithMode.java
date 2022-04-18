package edu.sdsc.datatype.execution;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;

public class PlanEdgeWithMode extends PlanEdge {
    private boolean isPipeline = false;

    public void setPipeline(boolean pipeline) {
        isPipeline = pipeline;
    }

    public boolean isPipeline() {
        return isPipeline;
    }
}

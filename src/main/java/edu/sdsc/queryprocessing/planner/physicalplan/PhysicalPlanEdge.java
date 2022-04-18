package edu.sdsc.queryprocessing.planner.physicalplan;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;

import java.util.List;

public class PhysicalPlanEdge {
    private Integer processorID;
    private List<Integer> variables;
    private boolean hasValue = false;
    private Integer childrenTouch;

    public PhysicalPlanEdge(PlanEdge p, Integer processorID) {
        this.processorID = processorID;
        this.variables = p.getVariables();
        this.childrenTouch = processorID;
    }


    public void setProcessorID(Integer processorID) {
        this.processorID = processorID;
    }

    public Integer getProcessorID() {
        return processorID;
    }

    public void setVariables(List<Integer> variables) {
        this.variables = variables;
    }

    public List<Integer> getVariables() {
        return variables;
    }

    public boolean isHasValue() {
        return hasValue;
    }

    public void setHasValue(boolean hasValue) {
        this.hasValue = hasValue;
    }

    public void setChildrenTouch(Integer childrenTouch) {
        this.childrenTouch = childrenTouch;
    }

    public Integer getChildrenTouch() {
        return childrenTouch;
    }
}


package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements;

import org.jgrapht.graph.DefaultEdge;

import java.util.List;

public class PlanEdge extends DefaultEdge {
    private List<Integer> variables;

    public List<Integer> getVariables() {
        return variables;
    }

    public void setVariables(List<Integer> variables) {
        this.variables = variables;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}

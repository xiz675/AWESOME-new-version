package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Operator;

public class HighLevelOperator extends Operator {
    private Integer inputOperator;

    public void setInputOperator(Integer inputOperator) {
        this.inputOperator = inputOperator;
    }

    public Integer getInputOperator() {
        return inputOperator;
    }

}


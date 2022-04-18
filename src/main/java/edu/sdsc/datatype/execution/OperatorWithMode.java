package edu.sdsc.datatype.execution;

import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;

public class OperatorWithMode {
    private PhysicalOperator operator;
    private boolean isParallel;
    private PipelineMode mode;

    public OperatorWithMode(PhysicalOperator op, PipelineMode mode) {
        setOperator(op);
        setMode(mode);
    }

    public void setOperator(PhysicalOperator operator) {
        this.operator = operator;
    }

    public void setParallel(boolean parallel) {
        isParallel = parallel;
    }

    public PhysicalOperator getOperator() {
        return operator;
    }

    public boolean isParallel() {
        return isParallel;
    }

    public void setMode(PipelineMode mode) {
        this.mode = mode;
    }

    public PipelineMode getMode() {
        return mode;
    }
}

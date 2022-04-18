package edu.sdsc.queryprocessing.planner.physicalplan.createplan;

public class TestNode {
    private Integer processorID = -1;
    private Integer childrenTouch = -1;
    private boolean occupied = false;
    private boolean hasValue = false;
    private Integer id;

    public TestNode(Integer id) {
        this.id = id;
    }


    public boolean isHasValue() {
        return hasValue;
    }

    public void setHasValue(boolean hasValue) {
        this.hasValue = hasValue;
    }

    public boolean isOccupied() {
        return occupied;
    }

    public void setOccupied(boolean occupied) {
        this.occupied = occupied;
    }

    public void setProcessorID(Integer processorID) {
        this.processorID = processorID;
        this.childrenTouch = processorID;
    }

    public void changeProcessorID(Integer processorID) {
        this.processorID = processorID;
    }

    public void setChildrenTouch(Integer childrenTouch) {
        this.childrenTouch = childrenTouch;
    }

    public Integer getProcessorID() {
        return processorID;
    }

    public void increaseChildrenTouch() {
        this.childrenTouch = this.childrenTouch+1;
    }

    public Integer getChildrenTouch() {
        return childrenTouch;
    }

}

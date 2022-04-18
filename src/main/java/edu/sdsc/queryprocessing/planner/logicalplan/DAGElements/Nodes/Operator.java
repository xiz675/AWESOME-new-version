package edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class Operator {
    private Integer id;
//    private boolean isFunc = false;
    private Set<Integer> inputVar = new HashSet<>();
    private Set<Integer> outputVar = new HashSet<>();
    private boolean returnNode = false;
//    private Integer processorID = -1;
    private List<Integer> variables;
//    private boolean hasValue = false;
//    private Integer childrenTouch = -1;
//    private boolean occupied = false;
    private boolean returnOperator = false;
    private Integer outputOperator;
    private List<Integer> blockUsed = new ArrayList<>();
    private Integer blockID = -1;
    private boolean subOperator = false;

//    public boolean isFunc() {
//        return isFunc;
//    }
//
//    public void setFunc(boolean func) {
//        isFunc = func;
//    }


    public void setBlockID(Integer blockID) {
        this.subOperator = true;
        this.blockID = blockID;
    }

    public Integer getBlockID() {
        return blockID;
    }

    public boolean isSubOperator() {
        return subOperator;
    }

    public void setSubOperator(boolean subOperator) {
        this.subOperator = subOperator;
    }

//    public boolean isOccupied() {
//        return occupied;
//    }
//
//    public void setOccupied(boolean occupied) {
//        this.occupied = occupied;
//    }

//    public void setProcessorID(Integer processorID) {
//        this.processorID = processorID;
//        this.childrenTouch = processorID;
//    }

//    public void changeProcessorID(Integer processorID) {
//        this.processorID = processorID;
//    }

    public void setBlockUsed(List<Integer> blockUsed) {
        this.blockUsed = blockUsed;
    }

    public List<Integer> getBlockUsed() {
        return blockUsed;
    }

    public void addBlockUsed(Integer bID){
        this.blockUsed.add(bID);
    }

    public void addBlocksUsed(List<Integer> bIDs) {
        this.blockUsed.addAll(bIDs);
    }

//    public void setChildrenTouch(Integer childrenTouch) {
//        this.childrenTouch = childrenTouch;
//    }
//    public Integer getProcessorID() {
//        return processorID;
//    }

    public void setVariables(List<Integer> variables) {
        this.variables = variables;
    }

    public List<Integer> getVariables() {
        return variables;
    }

//    public boolean isHasValue() {
//        return hasValue;
//    }
//
//    public void setHasValue(boolean hasValue) {
//        this.hasValue = hasValue;
//    }

//    public void increaseChildrenTouch() {
//        this.childrenTouch = this.childrenTouch+1;
//    }
//
//    public Integer getChildrenTouch() {
//        return childrenTouch;
//    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setInputVar(Set<Integer> inputVar) {
        this.inputVar = inputVar;
    }

    public void setOutputVar(Set<Integer> outputVar) {
        this.outputVar = outputVar;
    }
    public void addInputVariables(Set<Integer> inputs) {
        this.inputVar.addAll(inputs);
    }
    public void addInputVariable(Integer input) {
        this.inputVar.add(input);
    }

    public  Set<Integer> getInputVar() {
        return this.inputVar;
    }

    public  Set<Integer> getOutputVar() {
        return this.outputVar;
    }


    public boolean isReturnOperator() {
        return returnOperator;
    }

    // todo: what is the difference between subOperator and returnOperator
    public void setReturnOperator() {
        this.returnOperator = true;
    }
    public void setOutputOperator(Integer outputOperator) {
        this.outputOperator = outputOperator;
    }

    public Integer getOutputOperator() {
        return outputOperator;
    }
    public boolean isReturnNode() {
        return returnNode;
    }

    public void setReturnNode(boolean returnNode) {
        this.returnNode = returnNode;
    }
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}

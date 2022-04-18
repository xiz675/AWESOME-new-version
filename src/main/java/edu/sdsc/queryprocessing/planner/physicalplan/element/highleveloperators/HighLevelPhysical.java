package edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators;

import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.physicalVar;

// set the inner operator id and data variable id, and the input operator ids
public abstract class HighLevelPhysical extends PhysicalOperator {
    private Set<Integer> inputOperator = new HashSet<>();
    private Set<PhysicalOperator> subOperators = new HashSet<>();
    private Integer appliedVarID;
    private List<String> localVar;
    // todo: get this based on parsing results
    private List<Integer> localVarID;
    // todo: get this from parsing and also should use DataTypeEnum
    private DataTypeEnum elementType = DataTypeEnum.Undecided;



    public HighLevelPhysical() {

    }

    public HighLevelPhysical(Integer appliedVarID, Integer inputOperator, Set<Integer> inputVars){
        this.appliedVarID = appliedVarID;
        this.inputOperator.add(inputOperator);
        this.setInputVar(physicalVar(inputVars));
//        this.localVar = localVar;
    }

    public HighLevelPhysical(Integer appliedVarID, Integer inputOperator, Set<Integer> inputVars, List<Integer> localVarID, DataTypeEnum elementType){
        this.appliedVarID = appliedVarID;
        this.inputOperator.add(inputOperator);
        this.setInputVar(physicalVar(inputVars));
        this.localVarID = localVarID;
        this.elementType = elementType;
//        this.localVar = localVar;
    }

    public List<Integer> getLocalVarID() {
        return localVarID;
    }

    public void setLocalVarID(List<Integer> localVarID) {
        this.localVarID = localVarID;
    }

    public void setElementType(DataTypeEnum elementType) {
        this.elementType = elementType;
    }

    public DataTypeEnum getElementType() {
        return elementType;
    }

    public void addInputOperator(Integer inputOperator) {
        this.inputOperator.add(inputOperator);
    }

    public Set<Integer> getInputOperator() {
        return inputOperator;
    }

    public Integer getAppliedVarID() {
        return appliedVarID;
    }

    public void setAppliedVarID(Integer appliedVarID) {
        this.appliedVarID = appliedVarID;
    }


    public void setLocalVar(List<String> localVar) {
        this.localVar = localVar;
    }

    public List<String> getLocalVar() {
        return localVar;
    }

    public void setSubOperators(Set<PhysicalOperator> subOperator) {
        this.subOperators = subOperator;
    }

    public Set<PhysicalOperator> getSubOperators() {
        return this.subOperators;
    }

    public void addSubOperator(PhysicalOperator ope) {
        this.subOperators.add(ope);
    }
}

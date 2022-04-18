package edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.highleveloperators.BiOperationExecutor;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.planner.physicalplan.createplan.CreatePhysicalPlan.findOpeByID;

public class BiOpePhysical extends HighLevelPhysical{
    private Integer leftOperatorId;
    private Integer rightOperatorId;
    private String biOperation;
    private PhysicalOperator leftOpe;
    private PhysicalOperator rightOpe;

    public BiOpePhysical(String biOpe, Integer lID, Integer rID) {
//        super();
        this.leftOperatorId = lID;
        this.rightOperatorId = rID;
        this.biOperation = biOpe;
        this.addInputOperator(lID);
        this.addInputOperator(rID);
        // todo: add element type later
    }

    public Integer getLeftOperatorId() {
        return leftOperatorId;
    }

    public Integer getRightOperatorId() {
        return rightOperatorId;
    }

    public PhysicalOperator getLeftOpe() {
        return leftOpe;
    }

    public PhysicalOperator getRightOpe() {
        return rightOpe;
    }

    public void setLeftOpe(PhysicalOperator leftOpe) {
        this.leftOpe = leftOpe;
    }

    public void setRightOpe(PhysicalOperator rightOpe) {
        this.rightOpe = rightOpe;
    }

    public String getBiOperation() {
        return biOperation;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new BiOperationExecutor(this, getLeftOpe(), getRightOpe(), evt, vt, config, sqlCon, localEvt);
    }
}

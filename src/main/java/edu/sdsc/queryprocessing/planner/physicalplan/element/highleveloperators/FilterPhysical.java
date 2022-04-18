package edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators;

import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.highleveloperators.FilterExecutor;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.Set;

public class FilterPhysical extends HighLevelPhysical {
    // todo: since filter can accept text matrix, should set applied varType set when parsing

    private DataTypeEnum appliedVarType = DataTypeEnum.List;
    private PhysicalOperator innerOpe;

    public PhysicalOperator getInnerOpe() {
        return innerOpe;
    }

    public void setInnerOpe(PhysicalOperator innerOpe) {
        this.innerOpe = innerOpe;
    }

    public FilterPhysical(Integer appliedVarID, Integer inputOperator, Set<Integer> inputVar) {
        super(appliedVarID, inputOperator, inputVar);
        // todo: will change it later
        this.setPipelineCapability(PipelineMode.block);
        this.setParallelCapability(ParallelMode.parallel);
    }

    public FilterPhysical(Integer appliedVarID, Integer inputOperator, Set<Integer> inputVar, List<Integer> localVarID, DataTypeEnum appliedVarType, DataTypeEnum elementType) {
        super(appliedVarID, inputOperator, inputVar, localVarID, elementType);
        this.setAppliedVarType(appliedVarType);
        this.setPipelineCapability(PipelineMode.block);
        this.setParallelCapability(ParallelMode.parallel);

    }

    public void setAppliedVarType(DataTypeEnum appliedVarType) {
        this.appliedVarType = appliedVarType;
    }

    public DataTypeEnum getAppliedVarType() {
        return appliedVarType;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new FilterExecutor(this, evt, vt, config, sqlCon, localEvt);
    }
}

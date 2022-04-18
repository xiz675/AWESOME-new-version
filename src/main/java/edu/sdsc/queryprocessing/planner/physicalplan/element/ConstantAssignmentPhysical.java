package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ConstantAssignment;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.ExecuteConstantAssign;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class ConstantAssignmentPhysical extends PhysicalOperator {
    private Object value;
    public ConstantAssignmentPhysical(ConstantAssignment oprt) {
        value = oprt.getValue();
        setOutputVar(PlanUtils.physicalVar(oprt.getOutputVar()));
    }

    public Object getValue() {
        return value;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new ExecuteConstantAssign(this);
    }
}

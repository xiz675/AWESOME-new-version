package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Union;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class UnionPhysical extends FunctionPhysicalOperator{
    public UnionPhysical(Union union) {
        this.setParameters(union.getParameters());
        this.setInputVar(PlanUtils.physicalVar(union.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(union.getOutputVar()));
    }

    @Override
    public AwesomeRunnable createExecutor( ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

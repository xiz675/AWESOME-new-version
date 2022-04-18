package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.Tdm;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class TdmPhysical extends FunctionPhysicalOperator{
    public TdmPhysical(Tdm tdm) {
        this.setParameters(tdm.getParameters());
        this.setInputVar(PlanUtils.physicalVar(tdm.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(tdm.getOutputVar()));
    }

    @Override
    public AwesomeRunnable createExecutor( ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

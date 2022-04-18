package edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.BlackBoxFunc;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class BlackBoxPhysical extends FunctionPhysicalOperator {
    private String funcName;
    public BlackBoxPhysical() {}
    public BlackBoxPhysical(BlackBoxFunc b) {
        this.setParameters(b.getParameters());
        this.setFuncName(b.getFuncName());
        this.setInputVar(PlanUtils.physicalVar(b.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(b.getOutputVar()));
    }

    public String getFuncName() {
        return funcName;
    }

    public void setFuncName(String funcName) {
        this.funcName = funcName;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

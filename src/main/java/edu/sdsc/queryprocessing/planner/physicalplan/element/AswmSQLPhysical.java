package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.DBPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.AwsmSQL;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class AswmSQLPhysical extends DBPhysical {
    public AswmSQLPhysical(AwsmSQL x) {
        this.setStatement(x.getStatement());
        this.setVarPosition(x.getVarPosition());
        this.setInputVar(PlanUtils.physicalVar(x.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(x.getOutputVar()));
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

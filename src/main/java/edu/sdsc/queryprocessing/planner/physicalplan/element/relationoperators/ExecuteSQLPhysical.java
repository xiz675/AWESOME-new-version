package edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ExecuteSQL;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;


public class ExecuteSQLPhysical extends DBPhysical{

    public ExecuteSQLPhysical(ExecuteSQL x) {
        this.setPipelineCapability(PipelineMode.streamoutput);
        this.setStatement(x.getStatement());
        this.setVarPosition(x.getVarPosition());
        this.setSchema(x.getSchema());
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExecuteSQL(config, this, evt, sqlCon, localEvt);
    }
}


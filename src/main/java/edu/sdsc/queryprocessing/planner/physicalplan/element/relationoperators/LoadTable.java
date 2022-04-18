package edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExeLoadTable;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class LoadTable extends PhysicalOperator {
    private String sourceDatabase;
    private String sourceTable;


    public LoadTable(String srcDB, String srcTB) {
        this.sourceDatabase = srcDB;
        this.sourceTable = srcTB;
    }

    public String getSourceDatabase() {
        return sourceDatabase;
    }

    public String getSourceTable() {
        return sourceTable;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new ExeLoadTable(this, config, sqlCon);
    }
}

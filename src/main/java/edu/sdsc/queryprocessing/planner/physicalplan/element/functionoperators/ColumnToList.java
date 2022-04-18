package edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class ColumnToList extends PhysicalOperator {
    private String colName;
    private String rName;
    public ColumnToList(String rName, String cName) {
        this.rName = rName;
        this.colName = cName;
    }

    public String getColName() {
        return colName;
    }

    public String getrName() {
        return rName;
    }

    @Override
    public AwesomeRunnable createExecutor( ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return null;
    }
}

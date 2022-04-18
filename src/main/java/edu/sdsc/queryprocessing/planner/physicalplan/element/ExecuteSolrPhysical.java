package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ExecuteSolr;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.DBPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

public class ExecuteSolrPhysical extends DBPhysical {
    List<String> cols;
    public ExecuteSolrPhysical(ExecuteSolr solr) {
        this.setExecuteUnit(solr.getDbName());
        this.cols = solr.getColumns();
        this.setStatement(solr.getStatement());
        this.setVarPosition(solr.getVarPosition());
        this.setInputVar(PlanUtils.physicalVar(solr.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(solr.getOutputVar()));
    }

    public List<String> getCols() {
        return cols;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExecuteSolr(config, this, evt, localEvt);
    }
}

package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.ExecuteSQLPhysical;
import edu.sdsc.utils.RDBMSUtils;
import org.jooq.*;
import org.jooq.impl.DSL;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Collectors;

import static edu.sdsc.utils.DBMSUtils.generateStatement;

public class ExecuteSQL extends AwesomeStreamOutputRunnable<ExecuteSQLPhysical, AwesomeRecord> {
    public JsonObject config;
    public Integer unitSize = 500;
    private String sqlStatement;
    private Connection fromCon;
    private boolean isLocalRelation = false;


    // since it can't be executed in parallel, there is no need for latch
    public ExecuteSQL(JsonObject config, ExecuteSQLPhysical ope, ExecutionVariableTable evt,
                      Connection sqlCon, ExecutionVariableTable... localEvt) {
        super(ope);
        PipelineMode mode = ope.getExecutionMode();
        this.setExecutionMode(mode);
        this.sqlStatement = generateStatement(ope, ope.getStatement(), evt, true, localEvt);
        String loc = ope.getExecuteUnit();
        if (loc.equals("*")) {
            this.fromCon = sqlCon;
            this.isLocalRelation = true;
        }
        else {
            RDBMSUtils db_util = new RDBMSUtils(config, loc);
            this.fromCon = db_util.getConnection();
        }
    }

    @Override
    public void executeBlocking() {
        DSLContext create = execute();
        setMaterializedOutput(create.resultQuery(this.sqlStatement).stream().map(AwesomeRecord::new).collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        DSLContext create = execute();
        setStreamResult(create.resultQuery(this.sqlStatement).stream().map(AwesomeRecord::new));
    }

    private DSLContext execute() {
        DSLContext create;
        if (this.isLocalRelation) {
            create = DSL.using(fromCon, SQLDialect.SQLITE);
        }
        else{
            create = DSL.using(fromCon, SQLDialect.POSTGRES);
        }
        return create;
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (!this.isStreamOut()) {
            return new MaterializedRelation(this.getMaterializedOutput());
        }
        else {
            return new StreamRelation(this.getStreamResult());
        }
    }



    public static void main(String[] args) {

    }

}

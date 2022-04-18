package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.Store2SQLite;
import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExeStore;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.RDBMSUtils;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import javax.xml.stream.util.StreamReaderDelegate;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class Materialize extends PhysicalOperator {
    private String trgtMachine;
    private Integer varID;

    public String tableName;
    public String getTrgtMachine() {
        return trgtMachine;
    }

    public Materialize(String tbName, Integer id, String target) {
        this.varID = id;
        this.trgtMachine = target;
        this.tableName = tbName;
    }

    public String getTableName() {
        return tableName;
    }

    // no direct value cause it can't be parallelized
    @Override
    public ExeStore createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        RDBMSUtils db_util = new RDBMSUtils(config, this.getTrgtMachine());
        Connection toCon = db_util.getConnection();
        ExeStore executor;
        try {
            toCon.setAutoCommit(false);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        String tbName = this.getTableName();
        if (this.isStreamInput()) {
            StreamRelation inputTE = (StreamRelation) getTableEntryWithLocal(this.getInputVar().iterator().next(), evt, localEvt).getValue();
            executor = new ExeStore(inputTE.getValue(), toCon, tbName);
        }
        else {
            List<AwesomeRecord> input;
            ExecutionTableEntryMaterialized temp = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(this.getInputVar().iterator().next(), evt, localEvt);
            if (temp.isPartitioned()) {
                List<List<AwesomeRecord>> tempValue = (List<List<AwesomeRecord>>) temp.getPartitionedValue();
                input = mergeData(tempValue);
            }
            else {
                input = (List<AwesomeRecord>) temp.getValue();
            }
            executor = new ExeStore(input, toCon, tbName);
        }
        return executor;
    }
}

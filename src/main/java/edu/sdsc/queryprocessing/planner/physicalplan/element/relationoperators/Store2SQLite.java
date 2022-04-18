package edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.element.Materialize;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExeStore;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.utils.RDBMSUtils;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class Store2SQLite extends PhysicalOperator {
    private String tableName;
    public Store2SQLite(String tn) {
        tableName = tn;
        this.setPipelineCapability(PipelineMode.streaminput);
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        ExeStore executor;
        String tbName = this.getTableName();
        if (this.isStreamInput()) {
            Stream<AwesomeRecord> input;
            if (directValue) {
                input = (Stream<AwesomeRecord>) inputValue;
            }
            else {
                input = ((StreamRelation) getTableEntryWithLocal(this.getInputVar().iterator().next(), evt, localEvt)).getValue();
            }
            executor = new ExeStore(input, sqlCon, tbName);
        }
        else {
            List<AwesomeRecord> input;
            if (directValue) {
                input = (List<AwesomeRecord>) inputValue;
            }
            else {
                ExecutionTableEntryMaterialized temp = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(this.getInputVar().iterator().next(), evt, localEvt);
                if (temp.isPartitioned()) {
                    List<List<AwesomeRecord>> tempValue = (List<List<AwesomeRecord>>) temp.getPartitionedValue();
                    input = mergeData(tempValue);
                }
                else {
                    input = (List<AwesomeRecord>) temp.getValue();
                }
            }
            executor = new ExeStore(input, sqlCon, tbName);
        }
        return executor;
    }
}

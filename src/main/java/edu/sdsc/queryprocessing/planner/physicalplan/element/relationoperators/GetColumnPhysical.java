package edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.GetColumn;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// mark: finished assigning parameters, cap on
public class GetColumnPhysical extends PhysicalOperator {
    private String colName;
    private String rName;
    private Pair<Integer, String> relationID;

    public GetColumnPhysical(Integer rID, String rName, String cName) {
//        Set<Pair<Integer, String>> temp = new HashSet<>();
        Pair<Integer, String> tempVar = new Pair<>(rID, "*");
//        temp.add(tempVar);
//        this.setInputVar(temp);
        this.relationID = tempVar;
        this.setCapOnVarID(tempVar);
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.sequential);
        this.rName = rName;
        this.colName = cName;
    }

    public String getColName() {
        return colName;
    }

    public String getrName() {
        return rName;
    }

    public Pair<Integer, String> getRelationID() {
        return relationID;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        GetColumn executor;
        Pair<Integer, String> inputVar = this.getInputVar().iterator().next();
        if (!this.isStreamInput()) {
            List<AwesomeRecord> input;
            if (directValue) {
                input = (List<AwesomeRecord>) inputValue;
            }
            else {
                input = ((MaterializedRelation) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new GetColumn(input, this.getColName());
            }
            else {
                executor = new GetColumn(input, this.getColName(), true);
            }
        }
        else {
            Stream<AwesomeRecord> input;
            if (directValue) {
                input = (Stream<AwesomeRecord>) inputValue;
            }
            else {
                input = ((StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamInput()) {
                executor = new GetColumn(input, this.getColName());
            }
            else {
                executor = new GetColumn(input, this.getColName(), true);
            }
        }
        return executor;
    }
}

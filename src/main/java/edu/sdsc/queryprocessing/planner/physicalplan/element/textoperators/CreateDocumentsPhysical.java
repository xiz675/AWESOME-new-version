package edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.text.CreateDocuments;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class CreateDocumentsPhysical extends PhysicalOperator {
    public String docIDCol;
    public String textColName;
    public String rName;

    public CreateDocumentsPhysical(String relation, String text, String id) {
        this.setPipelineCapability(PipelineMode.pipeline);
        this.docIDCol = id;
        this.rName = relation;
        this.textColName = text;
        this.setParallelCapability(ParallelMode.parallel);
    }

    // there should be four types of
    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        CreateDocuments executor;
        Pair<Integer, String> inputVar = getInput(this);
        if (this.isStreamInput()) {
            Stream<AwesomeRecord> input;
            // check if direct value
            if (directValue) {
                input = (Stream<AwesomeRecord>) inputValue;
            }
            else {
                input = ((StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new CreateDocuments(input, this.docIDCol, this.textColName, true);
            }
            else {
                executor = new CreateDocuments(input, this.docIDCol, this.textColName);
            }

        }
        else {
            List<AwesomeRecord> input;
            if (directValue) {
                input = (List<AwesomeRecord>) inputValue;
            }
            else {
                input = ((MaterializedRelation) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new CreateDocuments(input, this.docIDCol, this.textColName);
            }
            else {
                executor = new CreateDocuments(input, this.docIDCol, this.textColName, true);
            }
        }
        return executor;
    }
}

package edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedDocuments;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamDocuments;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.text.SplitDocuments;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// mark : finished cap-on
public class SplitDocumentsPhysical extends PhysicalOperator {
    private String splitter;

    public SplitDocumentsPhysical(String s) {
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
        splitter = s;
        this.setParallelCapability(ParallelMode.parallel);
    }

    public String getSplitter() {
        return splitter;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        SplitDocuments executor;
        Pair<Integer, String> inputVar = getInput(this);
        if (this.isStreamInput()) {
            Stream<Document> input;
            // check if direct value
            if (directValue) {
                input = (Stream<Document>) inputValue;
            }
            else {
                input = ((StreamDocuments) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new SplitDocuments(input, this.getSplitter(), true);
            }
            else {
                executor = new SplitDocuments(input, this.getSplitter());
            }
        }
        else {
            List<Document> input;
            if (directValue) {
                input = (List<Document>) inputValue;
            }
            else {
                input = ((MaterializedDocuments) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new SplitDocuments(input, this.getSplitter());
            }
            else {
                executor = new SplitDocuments(input, this.getSplitter(), true);
            }
        }
        return executor;
    }
}

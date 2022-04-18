package edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedDocuments;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamDocuments;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.text.FilterDocuments;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class FilterDocumentsPhysical extends PhysicalOperator {
    private String stopwordsFile;
    private List<String> stopwords = new ArrayList<>();

    public FilterDocumentsPhysical(){
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
    }



    public FilterDocumentsPhysical(String path) {
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
        setStopwordsFile(path);
        this.setParallelCapability(ParallelMode.parallel);
    }

    private void setStopwordsFile(String stopwords) {
        this.stopwordsFile = stopwords;
    }

    public String getStopwordsFile() {
        return stopwordsFile;
    }

    public List<String> getStopwords() {
        return stopwords;
    }

    public void setStopwords(List<String> stopwords) {
        this.stopwords = stopwords;
    }

    @Override
    public FilterDocuments createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        FilterDocuments executor;
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
                executor = new FilterDocuments(input, this.getStopwords(), true);
            }
            else {
                executor = new FilterDocuments(input, this.getStopwords());
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
                executor = new FilterDocuments(input, this.getStopwords());
            }
            else {
                executor = new FilterDocuments(input, this.getStopwords(), true);
            }
        }
        return executor;
    }
}

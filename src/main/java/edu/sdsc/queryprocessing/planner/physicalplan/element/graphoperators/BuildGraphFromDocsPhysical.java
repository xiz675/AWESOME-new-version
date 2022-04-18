package edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedDocuments;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamDocuments;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.BuildWordNeighborGraph;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.graph.CollectGraphElementFromDocs;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.getParameterWithKey;
import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class BuildGraphFromDocsPhysical extends FunctionPhysicalOperator {

    private FunctionParameter maxDistance;
    private FunctionParameter words;
    private Pair<Integer, String> docID;
    private List<String> wordsValue = null;

    // mark: cap-on
    public BuildGraphFromDocsPhysical(BuildWordNeighborGraph oprt) {
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
        List<FunctionParameter> parameters = translateParameters(oprt.getParameters());
        this.docID = new Pair<>(parameters.get(0).getVarID(), "*");
        // set cap on variable
        this.setCapOnVarID(this.docID);
        this.maxDistance = getParameterWithKey(parameters, "maxDistance");
        this.words = getParameterWithKey(parameters, "words");
        this.setInputVar(PlanUtils.physicalVar(oprt.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(oprt.getOutputVar()));
    }

    public FunctionParameter getMaxDistance() {
        return maxDistance;
    }

    public FunctionParameter getWords() {
        return words;
    }

    public Pair<Integer, String> getDocID() {
        return docID;
    }

    public List<String> getWordsValue() {
        return wordsValue;
    }

    public void setWordsValue(List<String> wordsValue) {
        this.wordsValue = wordsValue;
    }

    @Override
    public CollectGraphElementFromDocs createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        CollectGraphElementFromDocs executor;
        Pair<Integer, String> inputVar = getInput(this);
        int dis = (int) this.getMaxDistance().getValueWithExecutionResult(evt, localEvt);
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
                executor = new CollectGraphElementFromDocs(input, dis, this.getWordsValue(), true);
            }
            else {
                executor = new CollectGraphElementFromDocs(input, dis, this.getWordsValue());
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
                executor = new CollectGraphElementFromDocs(input, dis, this.getWordsValue());
            }
            else {
                executor = new CollectGraphElementFromDocs(input, dis, this.getWordsValue(), true);
            }

        }
        return executor;
    }
}

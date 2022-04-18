package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamDocuments;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.LDA;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.text.ExeLDA;
import edu.sdsc.queryprocessing.runnableexecutors.text.ExeNER;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.getParameterWithKey;
import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.*;

// mark: finished cap-on
public class LDAPhysical extends FunctionPhysicalOperator{
    private FunctionParameter num;
    private Pair<Integer, String> docID;

    public LDAPhysical(LDA lda) {
        this.setPipelineCapability(PipelineMode.streaminput);
        List<FunctionParameter> parameters = translateParameters(lda.getParameters());
        // get the documents varID and this should be the capability on variable
        this.docID = new Pair<>(parameters.get(0).getVarID(), "*");
        this.setCapOnVarID(this.docID);
        this.num = getParameterWithKey(parameters, "topic");
        this.setInputVar(physicalVar(lda.getInputVar()));
        this.setOutputVar(physicalVar(lda.getOutputVar()));
        this.setOutputWithOrder(physicalVarFromList(lda.getOutputWithOrder()));
    }

    public FunctionParameter getNum() {
        return num;
    }

    public Pair<Integer, String> getDocID() {
        return docID;
    }

    // the direct value can't be true cause it is non-parallel-able
    @Override
    public ExeLDA createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        if (this.isStreamInput()) {
            StreamDocuments inputTE = (StreamDocuments) evt.getTableEntry(docID);
            return new ExeLDA(this, evt, inputTE.getValue());
        }
        else {
            ExecutionTableEntryMaterialized listInput = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(docID, evt, localEvt);
            if (listInput.isPartitioned()) {
                List<List<Document>> tempValue = (List<List<Document>>) listInput.getPartitionedValue();
                List<Document> data = mergeData(tempValue);
                return new ExeLDA(this, evt, data);
            }
            else {
                return new ExeLDA(this, evt, (List<Document>) listInput.getValue());
            }
        }
    }
}

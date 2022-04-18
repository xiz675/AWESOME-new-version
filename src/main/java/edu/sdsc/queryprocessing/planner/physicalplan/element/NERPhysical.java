package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.NER;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.text.ExeNER;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;
import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ling.CoreLabel;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class NERPhysical extends FunctionPhysicalOperator{
    private Integer relationID;
    private String colName;
    private String model;
//    private AbstractSequenceClassifier<CoreLabel> model;


    public NERPhysical(NER ner) {
        this.setParallelCapability(ParallelMode.parallel);
        JsonArray parameter = ner.getParameters();
        JsonObject js = (JsonObject) parameter.get(0);
        this.relationID = js.getInt("varID");
        this.colName = js.getString("varName").split("\\.")[1];
        this.setInputVar(PlanUtils.physicalVar(ner.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(ner.getOutputVar()));
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getModel() {
        return model;
    }

    public Integer getRelationID() {
        return relationID;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        ExeNER executor;
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
            // pipeline
            if (this.isStreamOutput()) {
                executor = new ExeNER(input, this.getModel(), this.getColName(), true);
            }
            else {
                executor = new ExeNER(input, this.getModel(), this.getColName());
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
                executor = new ExeNER(input, this.getModel(), this.getColName());
            }
            else {
                executor = new ExeNER(input, this.getModel(), this.getColName(), true);
            }
        }
        return executor;
    }
}

package edu.sdsc.queryprocessing.planner.physicalplan.element.stringoperators;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamList;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.PageRank;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.StringFlat;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.stringoperators.ExeStringFlat;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class StringFlatPhysical extends FunctionPhysicalOperator {
//    // when these parameters are variables
    private FunctionParameter stringList;
    private FunctionParameter concatStringID;
    // true values, assign them if they are constant
    private String stringValue = null;
    private List<String> stringListValue = null;

    public StringFlatPhysical(StringFlat pr) {
        this.setParallelCapability(ParallelMode.parallel);
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParameters(pr.getParameters());
        this.setInputVar(PlanUtils.physicalVar(pr.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(pr.getOutputVar()));
        // todo: needs to know if the parameter is a variable or a constant
        this.setPipelineCapability(PipelineMode.pipeline);
        List<FunctionParameter> parameters = translateParameters(pr.getParameters());
        assert parameters.size()==2;
        this.concatStringID = parameters.get(0);
        if (!this.concatStringID.isVar()) {
            this.stringValue = (String) this.concatStringID.getValue();
        }
        this.stringList = parameters.get(1);
        if (!this.stringList.isVar()) {
            this.stringListValue = (List<String>) this.stringList.getValue();
        }
    }

    public void setStringListValue(List<String> stringListValue) {
        this.stringListValue = stringListValue;
    }

    public void setStringValue(String stringValue) {
        this.stringValue = stringValue;
    }

    public List<String> getStringListValue() {
        return stringListValue;
    }

    public String getStringValue() {
        return stringValue;
    }

    public FunctionParameter getConcatStringID() {
        return concatStringID;
    }

    public FunctionParameter getStringList() {
        return stringList;
    }

    @Override
    public ExeStringFlat createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        ExeStringFlat executor;
        Pair<Integer, String> inputVar = getInput(this);
        if (this.isStreamInput()) {
            Stream<String> input;
            // check if direct value
            if (directValue) {
                input = (Stream<String>) inputValue;
            }
            else {
                input = ((StreamList<String>) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            // pipeline
            if (this.isStreamOutput()) {
                executor = new ExeStringFlat(input, this.getStringValue(), true);
            }
            else {
                executor = new ExeStringFlat(input, this.getStringValue());
            }
        }
        else {
            List<String> input;
            if (directValue) {
                input = (List<String>) inputValue;
            }
            else {
                input = ((MaterializedList<String>) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isStreamOutput()) {
                executor = new ExeStringFlat(input, this.getStringValue());
            }
            else {
                executor = new ExeStringFlat(input, this.getStringValue(), true);
            }
        }
        return executor;
    }
}

package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryStream;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.SumOpe;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.SumList;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// mark: finished cap on variable
public class SumPhysical extends FunctionPhysicalOperator{
    // since sum only needs one parameter, directly assign variables to it
    // this is for sum operator which get a column as input, then the parameters and
    // todo: sum should be able to accept a constant value as input
    private boolean hasValue = false;
    private Pair<Integer, String> sumVarID;
    private String type = "List";

    // this is for operators where the input has been changed and from a column
    public SumPhysical(Pair<Integer, String> var, SumOpe sum, String type) {
        // this changes the input variable
        this.setSumVarID(var);
        this.setCapOnVarID(var);
        this.setPipelineCapability(PipelineMode.streaminput);
        Set<Pair<Integer, String>> input = new HashSet<>();
        input.add(var);
        this.setInputVar(input);
        this.setType(type);
        // the output variable remains unchanged
        this.setOutputVar(PlanUtils.physicalVar(sum.getOutputVar()));
    }

    // this is for sum operator which does not need to get any change
    public SumPhysical(SumOpe sum, String type) {
        this.setParameters(sum.getParameters());
        Set<Pair<Integer, String>> input = PlanUtils.physicalVar(sum.getInputVar());
        this.setInputVar(input);
        this.setType(type);
        this.setSumVarID(input.iterator().next());
        this.setCapOnVarID(this.getSumVarID());
        this.setPipelineCapability(PipelineMode.streaminput);
        this.setOutputVar(PlanUtils.physicalVar(sum.getOutputVar()));
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    public void setSumVarID(Pair<Integer, String> sumVarID) {
        this.sumVarID = sumVarID;
    }

    public Pair<Integer, String> getSumVarID() {
        return sumVarID;
    }

    // do not have directvalue since it is not parallel-able
    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        String type = this.getType();
        assert type.equals("Column") || type.equals("List");
        // SumPhysical temp = (SumPhysical) opt;
        // create stream in executor
        Pair<Integer, String> inputVar = this.getSumVarID();
        if (this.isStreamInput()) {
            Stream input = ((ExecutionTableEntryStream) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            return new SumList(input);
        }
        else {
            List<Object> input;
            ExecutionTableEntryMaterialized listInput = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(inputVar, evt, localEvt);
            if (listInput.isPartitioned()) {
                List<List<Object>> tempValue = (List<List<Object>>) listInput.getPartitionedValue();
                input = mergeData(tempValue);
            }
            else {
                input = (List<Object>) listInput.getValue();
            }
            return new SumList(input);
        }
    }
}

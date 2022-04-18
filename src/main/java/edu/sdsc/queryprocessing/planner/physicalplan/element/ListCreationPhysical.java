package edu.sdsc.queryprocessing.planner.physicalplan.element;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ListCreation;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.function.Range;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;

import java.sql.Connection;
import java.util.List;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.physicalVar;

public class ListCreationPhysical extends PhysicalOperator {
    private Pair<Boolean, Integer> start;
    private Pair<Boolean, Integer> end;
    private Pair<Boolean, Integer> step;


    public ListCreationPhysical(ListCreation oprt) {
        start = getValues(oprt.getStart());
        end = getValues(oprt.getEnd());
        step = getValues(oprt.getStep());
        this.setOutputVar(physicalVar(oprt.getOutputVar()));
        this.setPipelineCapability(PipelineMode.streamoutput);
    }

    private Pair<Boolean, Integer> getValues(JsonObject val) {
        if (val.getBoolean("hasValue")) {
            return new Pair<>(true, val.getInt("value"));
        }
        else {
            return new Pair<>(false, val.getInt("varID"));
        }
    }

    public Pair<Boolean, Integer> getStart() {
        return start;
    }

    public Pair<Boolean, Integer> getEnd() {
        return end;
    }

    public Pair<Boolean, Integer> getStep() {
        return step;
    }

    @Override
    public Range createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        return new Range(this, evt, localEvt);
    }
}

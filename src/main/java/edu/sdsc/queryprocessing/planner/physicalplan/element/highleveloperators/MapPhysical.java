package edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators;


import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.highleveloperators.Map;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.DataTypeEnum;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.getInput;
import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class MapPhysical extends HighLevelPhysical {
    private PhysicalOperator innerOpe = null;
    private Pair<Integer, String> localVarPairID = null;
    private List<PhysicalOperator> subOperators = new ArrayList<>();
    private boolean hasGraph = false;
    private boolean pipeline = false;


    public MapPhysical(Integer appliedVarID, Integer inputOperator, Set<Integer> inputVar, boolean pipeline) {
        super(appliedVarID, inputOperator, inputVar);
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
        this.pipeline = pipeline;
    }

    public MapPhysical(Integer appliedVarID, Integer inputOperator, Set<Integer> inputVar, List<Integer> localVarID, DataTypeEnum elementType, boolean pipeline) {
        super(appliedVarID, inputOperator, inputVar, localVarID, elementType);
        this.setCapOnVarID(new Pair<>(appliedVarID, "*"));
        this.setPipelineCapability(PipelineMode.pipeline);
        this.setParallelCapability(ParallelMode.parallel);
        this.pipeline = pipeline;
    }

    public void setInnerOpe(PhysicalOperator innerOpe) {
        this.innerOpe = innerOpe;
    }

    public List<PhysicalOperator> getInnerOperators() {
        return this.subOperators;
    }

    public PhysicalOperator getInnerOpe() {
        return innerOpe;
    }

    public void setInnerOperators(List<PhysicalOperator> operators) {
        this.subOperators = operators;
        this.hasGraph = true;
    }

    public boolean isHasGraph() {
        return hasGraph;
    }

    public void setLocalVarPairID(Pair<Integer, String> var) {
        this.localVarPairID = var;
    }

    public Pair<Integer, String> getLocalVarPairID() {
        return localVarPairID;
    }

    // todo : should change this, and the execution of Map, should change inner operator to a list of chains and then execute each chain, each chain will output a local TE
    @Override
    public Map createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        Map executor;
        Pair<Integer, String> inputVar = getInput(this);
        if (this.isStreamInput()) {
            Stream input;
            if (directValue) {
                input = (Stream) inputValue;
            }
            else {
                input = ((ExecutionTableEntryStream) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            executor = new Map(this, config, sqlCon, this.getLocalVarPairID(), optimize, this.pipeline, evt, vt, input);
        }
        else {
            List input;
            if (directValue) {
                input = (List) inputValue;
            }
            else {
                ExecutionTableEntryMaterialized inputTE = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(inputVar, evt, localEvt);
                if (inputTE.isPartitioned()) {
                    input = mergeData(inputTE.getPartitionedValue());
                }
                else {
                    input = (List) inputTE.getValue();
                }
            }
            executor = new Map(this, config, sqlCon, this.getLocalVarPairID(), optimize, this.pipeline, evt, vt, input);
        }
        return executor;
    }
}

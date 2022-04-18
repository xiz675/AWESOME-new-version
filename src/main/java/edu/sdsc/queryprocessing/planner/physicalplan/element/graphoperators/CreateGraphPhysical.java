package edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators;

import edu.sdsc.datatype.execution.GraphElement;
import edu.sdsc.datatype.execution.ParallelMode;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamGraphData;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructJGraphTGraph;
import edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph;
import edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructTinkerpopGraph;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.mergeData;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

// mark: finished cap-on change
public class CreateGraphPhysical extends PhysicalOperator {
    private boolean tinkerpop = true;
    private boolean neo4j = false;
//    private Pair<Integer, String> graphDataVar;

    public CreateGraphPhysical(Set<Pair<Integer, String>> input) {
        this.setPipelineCapability(PipelineMode.streaminput);
        this.setParallelCapability(ParallelMode.sequential);
        this.setCapOnVarID(input.iterator().next());
        this.setInputVar(input);
    }

    public boolean isTinkerpop() {
        return tinkerpop;
    }

    public void setTinkerpop(boolean tinkerpop) {
        this.tinkerpop = tinkerpop;
    }

    public boolean isNeo4j() {
        return neo4j;
    }

    public void setNeo4j(boolean neo4j) {
        this.tinkerpop = !neo4j;
        this.neo4j = neo4j;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        AwesomeRunnable executor;
        Pair<Integer, String> inputVar = this.getInputVar().iterator().next();
        if (this.isStreamInput()) {
            Stream<GraphElement> input;
            if (directValue) {
                input = (Stream<GraphElement>) inputValue;
            }
            else {
                input = ((StreamGraphData) getTableEntryWithLocal(inputVar, evt, localEvt)).getValue();
            }
            if (this.isTinkerpop()) {
                executor = new ConstructTinkerpopGraph(input);
            }
            else if (this.isNeo4j()) {
                // todo: for executeCypher, needs to decide the TE type
                executor = new ConstructNeo4jGraph(input);
            }
            else {
                executor = new ConstructJGraphTGraph(input);
//                    executor.setMaterializedInput(data);
            }
        }
        else {
            List<GraphElement> input;
            if (directValue) {
                input = (List<GraphElement>) inputValue;
            }
            else {
                // todo: may determine if the input is of MaterializeGraphElement type
                ExecutionTableEntryMaterialized listInput = (ExecutionTableEntryMaterialized) getTableEntryWithLocal(inputVar, evt, localEvt);
                if (listInput.isPartitioned()) {
                    List<List<GraphElement>> tempValue = (List<List<GraphElement>>) listInput.getPartitionedValue();
                    input = mergeData(tempValue);
                }
                else {
                    input = (List<GraphElement>) listInput.getValue();
                }
            }
            if (this.isTinkerpop()) {
                executor = new ConstructTinkerpopGraph(input);
            }
            else if (this.isNeo4j()) {
                // todo: for executeCypher, needs to decide the TE type
                executor = new ConstructNeo4jGraph(input);
            }
            else {
                executor = new ConstructJGraphTGraph(input);
            }
        }
        return executor;
    }

    //    private void setGraphDataVar(Pair<Integer, String> graphDataVar) {
//        this.graphDataVar = graphDataVar;
//    }
//
//    public Pair<Integer, String> getGraphDataVar() {
//        return graphDataVar;
//    }
}

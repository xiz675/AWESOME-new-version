package edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators;

import edu.sdsc.datatype.execution.FunctionParameter;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.PageRank;
import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.graph.PageRankJGraphT;
import edu.sdsc.queryprocessing.runnableexecutors.graph.PageRankTinkerpop;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.jgrapht.graph.DefaultWeightedEdge;

import javax.json.JsonObject;
import java.sql.Connection;
import java.util.Iterator;
import java.util.List;

import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.getParameterWithKey;
import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class PageRankPhysical extends FunctionPhysicalOperator {
    private boolean tinkerpop = true;

    // todo: should add graph id and other parameters here and should have a function deciding if this is a graph in Neo4j or Tinkerpop
    public PageRankPhysical(PageRank pr) {
        this.setParameters(pr.getParameters());
        this.setInputVar(PlanUtils.physicalVar(pr.getInputVar()));
        this.setOutputVar(PlanUtils.physicalVar(pr.getOutputVar()));
        this.setPipelineCapability(PipelineMode.streamoutput);
    }

    public boolean isTinkerpop() {
        return tinkerpop;
    }

    public void setTinkerpop(boolean tinkerpop) {
        this.tinkerpop = tinkerpop;
    }

    @Override
    public AwesomeRunnable createExecutor(ExecutionVariableTable evt, JsonObject config, VariableTable vt, Connection sqlCon, boolean optimize, Object inputValue, boolean directValue, ExecutionVariableTable... localEvt) {
        AwesomeRunnable executor;
        List<FunctionParameter> parameters = translateParameters(this.getParameters());
        FunctionParameter topk = getParameterWithKey(parameters, "topk");
        Integer graphID = parameters.get(0).getVarID();
        boolean useTinkerpop = this.isTinkerpop();
        boolean limitedNumber = false;
        int numOfNodes = 0;
        if (topk != null) {
            limitedNumber = (boolean) topk.getValueWithExecutionResult(evt, localEvt);
        }
        if (limitedNumber) {
            numOfNodes = (Integer) getParameterWithKey(parameters, "num").getValueWithExecutionResult(evt, localEvt);
        }
        if (useTinkerpop) {
            Graph graph;
            if (directValue) {
                graph = (Graph) inputValue;
            }
            else {
                graph = (Graph) getTableEntryWithLocal(new Pair<>(graphID, "*"), evt, localEvt).getValue();
            }
            // print the number of vertices
            // Iterator<Vertex> s = graph.vertices();
            Iterator<Edge> s = graph.edges();
            int count = 0;
            while (s.hasNext()) {
                s.next();
                count=count+1;
            }
            System.out.println("page rank on tinkerpop graph with edge size " + count);
            // todo: add page rank on neo4j later, since there will be several graphs in the map, needs to have multiple databases
            if (limitedNumber) {
                executor = new PageRankTinkerpop(graph, numOfNodes);
            }
            else {executor = new PageRankTinkerpop(graph);}
        }
        else {
            org.jgrapht.Graph<String, DefaultWeightedEdge> graph;
            if (directValue) {
                graph = (org.jgrapht.Graph<String, DefaultWeightedEdge>) inputValue;
            }
            else {
                graph = (org.jgrapht.Graph<String, DefaultWeightedEdge>) getTableEntryWithLocal(new Pair<>(graphID, "*"), evt, localEvt).getValue();
            }
            System.out.println("page rank on jgrapht graph with edge size " + graph.edgeSet().size());
            if (limitedNumber) {
                executor= new PageRankJGraphT<>(graph, numOfNodes);
            }
            else {
                executor = new PageRankJGraphT<>(graph);
            }
        }
        return executor;
    }
}


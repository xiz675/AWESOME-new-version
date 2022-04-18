package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.Edge;
import edu.sdsc.datatype.execution.GraphElement;
import edu.sdsc.datatype.execution.Node;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeJGraph;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;
import org.jgrapht.Graph;
import org.jgrapht.graph.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph.mergeGraphHelper;

public class ConstructJGraphTGraph extends AwesomeStreamInputRunnable<GraphElement, Graph<String, DefaultWeightedEdge>> {

    public ConstructJGraphTGraph(PipelineMode mode) {
        super(mode);
    }

    public ConstructJGraphTGraph(List<GraphElement> input) {
        super(input);
    }

    public ConstructJGraphTGraph(Stream<GraphElement> input) { super(input); }

    @Override
    public void executeStreamInput() {
        Graph<String, DefaultWeightedEdge> graph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
        getStreamInput().forEach(e -> addGraph(e, graph));
        setMaterializedOutput(graph);
    }

    @Override
    public void executeBlocking() {
        List<GraphElement> materializedInput = getMaterializedInput();
        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
        materializedInput.forEach(i -> mergeGraphHelper(mergedGraph, i));
        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
        Graph<String, DefaultWeightedEdge> graph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
        for (GraphElement edge : graphEleSet) {
            addGraph(edge, graph);
        }
        setMaterializedOutput(graph);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeJGraph(this.getMaterializedOutput());
    }

    private void addGraph(GraphElement edge, Graph<String, DefaultWeightedEdge> graph) {
        assert edge instanceof Edge;
        Node source = ((Edge) edge).getFirstNode();
        Node target = ((Edge) edge).getSecondNode();
        String sourceValue = (String) source.getProperty("value");
        String targetValue = (String) target.getProperty("value");
        if (targetValue.equals(sourceValue)) {
            return ;
        }
        graph.addVertex(sourceValue);
        graph.addVertex(targetValue);
        DefaultWeightedEdge e = graph.addEdge(sourceValue, targetValue);
        if (e==null) {
            System.out.println(sourceValue);
            System.out.println(targetValue);
            System.out.println(edge.getProperty("count"));
            return ;
        }
        try {
            graph.setEdgeWeight(e, Double.parseDouble(edge.getProperty("count").toString()));
        } catch (Exception exe) {
            System.out.println(sourceValue);
            System.out.println(targetValue);
            exe.printStackTrace();
        }
    }

}

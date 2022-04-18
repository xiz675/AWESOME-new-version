package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.*;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeGraph;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph.mergeGraphHelper;

// todo: this is hardcoded. This is not used
public class ConstructTinkerpopGraph extends AwesomeStreamInputRunnable<GraphElement, Graph> {
    // since this can't be executed in parallel, it does not need multiple producers
//    final private Integer unitSize = 500;
//    private Graph graph;

    // stream input
    public ConstructTinkerpopGraph(Stream<GraphElement> input) {
        super(input);
    }

    // block input
    public ConstructTinkerpopGraph(List<GraphElement> input) {
        super(input);
    }

//    @Override
//    public Graph getResult() {
//        return graph;
//    }

    private static Vertex addVertex(Graph g, Node t) {
        Vertex node = g.addVertex(t.getLabel());
        Map<String, Object> props = t.getProperties();
        for (String key : props.keySet()) {
            node.property(key, props.get(key));
        }
        return node;
    }

//    // todo: if edge exist, then modify count, else add count
//    // todo: currently hard code it, should change to general case
    private static void addGraphElement(Graph graph, GraphElement ele) {
        assert ele instanceof Edge;
        Map<String, Vertex> nodes = new HashMap<>();
        Edge temp = (Edge) ele;
        Node source = temp.getFirstNode();
        Node target = temp.getSecondNode();
        String v1V = (String) source.getProperty("value");
        String v2V = (String) target.getProperty("value");
        Vertex v1;
        Vertex v2;
        if (nodes.containsKey(v1V)) {
            v1 = nodes.get(v1V);
        }
        else {
            v1 = graph.addVertex("Word");
            v1.property("value", v1V);
            nodes.put(v1V, v1);
        }
        if (nodes.containsKey(v2V)) {
            v2 = nodes.get(v2V);
        }
        else {
            v2 = graph.addVertex("Word");
            v2.property("value", v2V);
            nodes.put(v2V, v2);
        }
        // add edge
        v1.addEdge(temp.getLabel(), v2, temp.getPropertyArray());
    }

    @Override
    public void executeStreamInput() {
        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
        getStreamInput().forEach(i -> mergeGraphHelper(mergedGraph, i));
        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
        Graph graph = TinkerGraph.open();
        graphEleSet.forEach(x -> addGraphElement(graph, x));
        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMaterializedOutput(graph);
    }

    @Override
    public void executeBlocking() {
        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
        getMaterializedInput().forEach(i -> mergeGraphHelper(mergedGraph, i));
        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
        Graph graph = TinkerGraph.open();
        graphEleSet.forEach(x -> addGraphElement(graph, x));
        try {
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        setMaterializedOutput(graph);
        // System.out.println();
        // System.out.println("add " + input.size() + " elements : " + (System.currentTimeMillis() - start) + " ms");

    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new AwesomeGraph(this.getMaterializedOutput());
    }

}

//package edu.sdsc.queryprocessing.executor.execution.ExecutionUnit.graphoperations;
//
//import edu.sdsc.datatype.execution.GraphElement;
//import edu.sdsc.datatype.execution.Graphdata;
//import edu.sdsc.queryprocessing.executor.execution.ElementVariable.ExecutionGraph;
//import edu.sdsc.queryprocessing.executor.execution.ElementVariable.ExecutionGraphData;
//import edu.sdsc.queryprocessing.executor.execution.ElementVariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.executor.execution.ExecutionUnit.FunctionInputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planer.physicalplan.Element.ConstructGraphPhysical;
//import edu.sdsc.utils.Pair;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
//import org.jgrapht.graph.DefaultWeightedEdge;
//import org.jgrapht.graph.SimpleDirectedWeightedGraph;
//
//import java.util.*;
//import java.util.stream.Stream;
//
//public class ExeCreateWeightedGraph implements FunctionInputStreamExecutionBase<GraphElement> {
//    private ExecutionVariableTable evt;
//    private ConstructGraphPhysical ope;
//
//    public ExeCreateWeightedGraph(ConstructGraphPhysical ope, ExecutionVariableTable evt) {
//        this.ope = ope;
//        this.evt = evt;
//    }
//
//    @Override
//    public void execute() {
//        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        List<GraphElement> graph = ((ExecutionGraphData) evt.getTableEntry(inputVar)).getValue();
//        executeStreamInput(graph.stream());
//    }
//
//    @Override
//    public void executeStreamInput(Stream<GraphElement> input) {
//        if (!input.isParallel()) {input = input.parallel();}
//        SimpleDirectedWeightedGraph<String, DefaultWeightedEdge> graph = new SimpleDirectedWeightedGraph<>(DefaultWeightedEdge.class);
//        input.forEach(s-> {
//            if (s.hasEdge) {
//                graph.addVertex(s.source);
//                graph.addVertex(s.target);
//                DefaultWeightedEdge edge = new DefaultWeightedEdge();
//                graph.addEdge(s.source, s.target, edge);
//                graph.setEdgeWeight(edge, s.edgeWeight);
//            }
//            else {
//                graph.addVertex(s.source);
//            }
//        });
//        ExecutionGraph graphResult = new ExecutionGraph(graph);
//        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        evt.insertEntry(ouptVar, graphResult);
//    }
//
//    public static void main(String[] args) {
//        Graph graph = TinkerGraph.open(); //1.创建一个新的内存TinkerGraph并将其分配给该变量graph。
//        Vertex vadas = graph.addVertex("Person");
//        vadas.property("name", "xiuwen");
//        GraphTraversalSource g = graph.traversal();
//        g.addV().property("name", "marko").property("name", 1).next();
//        System.out.println(g.getGraph().vertices().next().properties("name"));
////        Map<String, Object> property = new HashMap<>();
////        property.put("x", 1);
////        property.put("y", "s");
////        System.out.println(property.toString());
//
//    }
//}

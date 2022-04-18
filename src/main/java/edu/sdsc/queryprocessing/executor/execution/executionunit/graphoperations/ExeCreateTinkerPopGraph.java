//package edu.sdsc.queryprocessing.executor.execution.executionunit.graphoperations;
//
//import edu.sdsc.datatype.execution.Edge;
//import edu.sdsc.datatype.execution.GraphElement;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.datatype.execution.Node;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.InputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.CreateGraphPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
//
//import java.util.*;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//// there should be two methods, one accept a stream input and return graph, the other one accept a block input and return
//// graph, whether to insert into a execution table or get input from execution table are in the main function \
//
//public class ExeCreateTinkerPopGraph extends InputStreamExecutionBase{
//
//    private List<GraphElement> materializedGraphData;
//    private StreamGraphData streamGraphData;
//
//    // put value assignment in the constructor so that each executor can share the same methods interface
//    public ExeCreateTinkerPopGraph() {
//        setExecutionMode(PipelineMode.block);
////        PipelineMode mode = ope.getExecutionMode();
////        this.setExecutionMode(mode);
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
////        if (mode.equals(PipelineMode.block)) {
////            this.materializedGraphData = ((MaterializedGraphData) getTableEntryWithLocal(inputVar, evt, localEvt));
////        }
////        else {
////            assert mode.equals(PipelineMode.streaminput);
////            this.streamGraphData = ((StreamGraphData) getTableEntryWithLocal(inputVar, evt, localEvt));
////        }
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        // List<GraphElement> graphData = materializedGraphData.getValue();
//        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
//        materializedGraphData.forEach(i -> {if (mergedGraph.containsKey(i)) {
//            int count = (Integer) mergedGraph.get(i).getProperty("count");
//            mergedGraph.get(i).addProperty("count", count+1);
//        }
//        else {
//            mergedGraph.put(i, i); } });
//        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
//
//        System.out.println("Create Tinkerpop graph with edge size: " + graphEleSet.size());
//        Graph graph = addGraphElement(graphEleSet);
////        for (GraphElement ge : graphEleSet) {
////            addGraphElement(graph, ge);
////        }
////        try {
////            graph.close();
////        } catch (Exception e) {
////            e.printStackTrace();
////        }
//        return new AwesomeGraph(graph);
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized executeStreamInput() {
////        Flowable<GraphElement> graphData = Flowable.fromPublisher(streamGraphData.getValue()).observeOn(Schedulers.computation());
//        Flowable<GraphElement> graphData = Flowable.fromPublisher(streamGraphData.getValue());
//        Graph graph = addGraphElement(graphData.toList().blockingGet());
//        return new AwesomeGraph(graph);
//    }
//
//    private static Vertex addVertex(Graph g, Node t) {
//        Vertex node = g.addVertex(t.getLabel());
//        Map<String, Object> props = t.getProperties();
//        for (String key : props.keySet()) {
//            node.property(key, props.get(key));
//        }
//        return node;
//    }
//
//    public static Graph addGraphElement(List<GraphElement> x) {
//        Graph graph = TinkerGraph.open();
//        GraphTraversalSource g = graph.traversal();
//        Map<String, Vertex> nodes = new HashMap<>();
//        for (GraphElement ele : x) {
////            if (ele instanceof Node) {
////                Node temp = ((Node) ele);
////                String vV = (String) temp.getProperty("value");
////                Optional<Vertex> v = g.V().has("value", vV).tryNext();
////                v.orElseGet(() -> graph.addVertex( "value", vV));
//////            addVertex(g, temp);
////            }
////            else {
//            assert ele instanceof Edge;
//            Edge temp = (Edge) ele;
//            Node source = temp.getFirstNode();
//            Node target = temp.getSecondNode();
//            String v1V = (String) source.getProperty("value");
//            String v2V = (String) target.getProperty("value");
//            Vertex v1;
//            Vertex v2;
//            if (nodes.containsKey(v1V)) {
//                v1 = nodes.get(v1V);
//            }
//            else {
//                v1 = graph.addVertex("Word");
//                v1.property("value", v1V);
//                nodes.put(v1V, v1);
//            }
//            if (nodes.containsKey(v2V)) {
//                v2 = nodes.get(v2V);
//            }
//            else {
//                v2 = graph.addVertex("Word");
//                v2.property("value", v2V);
//                nodes.put(v2V, v2);
//            }
//            // add edge
//            v1.addEdge(temp.getLabel(), v2, temp.getPropertyArray());
//        }
//        try {
//            g.close();
//            graph.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return graph;
//    }
//
//    public List<GraphElement> getMaterializedGraphData() {
//        return materializedGraphData;
//    }
//
//    public void setMaterializedGraphData(List<GraphElement> materializedGraphData) {
//        this.materializedGraphData = materializedGraphData;
//    }
//
//    public static void main(String[] args) {
//        Node n1 = new Node("xiuwen", "value", "xiuwen");
//        Node n2 = new Node("cat", "value", "cat");
//        Edge e1 = new Edge("x", "count", 1);
//        e1.setNodes(n1, n2, false);
////        Edge e2 = new Edge("y");
//        List<GraphElement> x = new ArrayList<>();
//        x.add(e1);
//        Node n3 = new Node("xiuwen", "value", "xiuwen");
//        Node n4 = new Node("cat", "value", "cat");
//        Edge e2 = new Edge("x", "count", 1);
//        e2.setNodes(n3, n4, false);
//        x.add(e2);
//
//        Node n5 = new Node("xiuwen", "value", "xiuwen");
//        Node n6 = new Node("dog", "value", "dog");
//        Edge e3 = new Edge("x", "count", 1);
//        e3.setNodes(n5, n6, false);
//        x.add(e3);
//
//        Set<Vertex> nodes = new HashSet<>();
//
//        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
//        x.forEach(i -> {if (mergedGraph.containsKey(i)) {
//            int count = (Integer) mergedGraph.get(i).getProperty("count");
//            mergedGraph.get(i).addProperty("count", count+1);
//        }
//        else {
//            mergedGraph.put(i, i); } });
//        List<GraphElement> graphEleSet = new ArrayList<>(mergedGraph.values());
//
//        Graph graph = TinkerGraph.open();
//        GraphTraversalSource g = graph.traversal();
//        for (GraphElement ge : graphEleSet) {
//            Edge temp = ((Edge) ge);
//            // add node if does not exist
//            Node source = temp.getFirstNode();
//            String v1V = (String) source.getProperty("value");
//            Optional<Vertex> v = g.V().has("value", v1V).tryNext();
//            Vertex v1 = v.orElseGet(() -> graph.addVertex( "value", v1V));
//
//            Node target = temp.getSecondNode();
//            String v2V = (String) target.getProperty("value");
//            Optional<Vertex> vv = g.V().has("value", v2V).tryNext();
//            Vertex v2 = vv.orElseGet(() -> graph.addVertex("value", v2V));
//
//            // add edge
//            v1.addEdge(temp.getLabel(), v2, temp.getPropertyArray());
//        }
//        try {
//            graph.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        System.out.println("s");
//    }
//}

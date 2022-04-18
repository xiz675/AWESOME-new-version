//package edu.sdsc.queryprocessing.runnableexecutors.graph;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.PairSecondComparator;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//
//import java.util.*;
//import java.util.concurrent.LinkedBlockingQueue;
//import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.bothE;
//
//
//public class Coreness extends AwesomeStreamOutputRunnable<Graph, Pair<Integer, Long>> {
//    Integer unitSize = 500;
//    private Map<Integer, Set<Integer>> neighbors;
//
//    public Coreness(Graph g, Map<Integer, Set<Integer>> nbs, LinkedBlockingQueue<List<Pair<Integer, Long>>> result) {
//        super(g, result);
//        this.neighbors = nbs;
//        setExecutionMode(PipelineMode.streamoutput);
//    }
//
//    public Coreness(Graph g, Map<Integer, Set<Integer>> nbs) {
//        super(g);
//        this.neighbors = nbs;
//        setExecutionMode(PipelineMode.block);
//    }
//
//    // a unit result is a list of Pairs with vertex id and coreness number
//    @Override
//    public void executeStreamOutput() {
//        Graph g = getMaterializedInput();
//        Integer size = unitSize;
//        LinkedBlockingQueue<List<Pair<Integer, Long>>> streamResult = getStreamResult();
//        GraphTraversal<Vertex, Map<String, Object>> v = g.traversal().V().project("v", "degree").by().by(bothE().count());
//        // nodes with degrees have already been sorted, needs to materialize the sorted iterator
////        List<Pair<Integer, Integer>> sortedVertex = v.map(i ->
////                new Pair<>((Integer) ((Vertex) i.get("v")), (Integer) i.get("degree")}))
//        List<Map<String, Object>> collectedVertex = v.toList();
//        Comparator<Pair<Integer, Long>> comparator = new PairSecondComparator();
//        PriorityQueue<Pair<Integer, Long>> sortedVertex = new PriorityQueue<>(collectedVertex.size(), comparator) ;
//        Map<Integer, Long> degreeMap = new HashMap<>();
//        getVertexDegree(collectedVertex, sortedVertex, degreeMap);
//        List<Pair<Integer, Long>> tempResult = new ArrayList<>();
//        try{
//            while (!sortedVertex.isEmpty()) {
//                if (size == 0) {
//                    streamResult.put(tempResult);
//                    tempResult = new ArrayList<>();
//                    size = unitSize;
//                }
//                Pair<Integer, Long> x =sortedVertex.remove();
//                tempResult.add(x);
//                size = size - 1;
//                Set<Integer> neighbor = this.neighbors.get(x.first);
//    //            List<Integer> nbrDegree = new ArrayList<>();
//                for (Integer id : neighbor) {
//                    Long tempND = degreeMap.get(id);
//                    if (tempND > x.second) {
//                        degreeMap.put(id, tempND - 1);
//                        sortedVertex.remove(new Pair<>(id, tempND));
//                        sortedVertex.add(new Pair<>(id, tempND-1));
//                    }
//                }
//            }
//            // for the last result
//            if (tempResult.size() > 0) {
////                for (Pair<Integer, Long> s : tempResult) {
////                    System.out.println(s.first + ": " + s.second);
////                }
//                streamResult.put(tempResult);
//            }
//            streamResult.put(Collections.emptyList());
//            System.out.println("Coreness countdown: " + getLatch().getCount() + " at " + System.currentTimeMillis());
//            countDown();
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void executeBlocking() {
//        Graph g = getMaterializedInput();
//        List<Pair<Integer, Long>> result = new ArrayList<>();
//        Map<Integer, Long> degreeMap = new HashMap<>();
//        GraphTraversal<Vertex, Map<String, Object>> v = g.traversal().V().project("v", "degree").by().by(bothE().count());
//        List<Map<String, Object>> collectedVertex = v.toList();
//        Comparator<Pair<Integer, Long>> comparator = new PairSecondComparator();
//        PriorityQueue<Pair<Integer, Long>> sortedVertex = new PriorityQueue<>(collectedVertex.size(), comparator);
//        getVertexDegree(collectedVertex, sortedVertex, degreeMap);
//        while (!sortedVertex.isEmpty()) {
//            Pair<Integer, Long> x =sortedVertex.remove();
//            result.add(x);
//            Set<Integer> neighbor = this.neighbors.get(x.first);
//            for (Integer id : neighbor) {
//                Long tempND = degreeMap.get(id);
//                if (tempND > x.second) {
//                    degreeMap.put(id, tempND - 1);
//                    sortedVertex.remove(new Pair<>(id, tempND));
//                    sortedVertex.add(new Pair<>(id, tempND-1));
//                }
//            }
//        }
//        setMaterializedOutput(result);
//    }
//
//
//    private void getVertexDegree(List<Map<String, Object>> collectedVertex, PriorityQueue<Pair<Integer, Long>> sortedVertex, Map<Integer, Long> degreeMap) {
//        for (Map<String, Object> temp : collectedVertex) {
//            Integer vid = ((Long) ((Vertex) temp.get("v")).id()).intValue();
//            Long degree = (Long) temp.get("degree");
//            sortedVertex.add(new Pair<>(vid, degree));
//            degreeMap.put(vid, degree);
//        }
//    }
//
//
//    public static void main(String[] args) {
//    }
//}

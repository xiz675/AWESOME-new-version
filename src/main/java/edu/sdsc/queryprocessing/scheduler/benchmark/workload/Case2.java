//package edu.sdsc.queryprocessing.scheduler.benchmark.workload;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.graph.AddNodeProperty;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.graph.CoreRank;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.graph.Coreness;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.graph.GetNeighbors;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.CoreRankMeta;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.CorenessMeta;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.ExecutorMetaData;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.NodePropertyMeta;
//import edu.sdsc.utils.Pair;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
//
//import java.io.BufferedReader;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.*;
//
//import static edu.sdsc.queryprocessing.scheduler.utils.ParallelPipelineUtil.singleBatchAssign;
//
//
//public class Case2 {
//
//    private static Graph createCoauthorGraph() {
//        Graph graph = TinkerGraph.open();
//        try{
//            BufferedReader buf = new BufferedReader(new FileReader("C:/Users/xw/Downloads/gemsec_facebook_dataset/artist_edges.csv"));
//            String lineJustFetched = null;
//            String[] wordsArray;
//            GraphTraversalSource g = graph.traversal();
//            Set<Integer> nodes = new HashSet<>();
//            while(true){
//                lineJustFetched = buf.readLine();
//                if(lineJustFetched == null){
//                    break;
//                } else{
//                    wordsArray = lineJustFetched.split(",");
//                    Integer v1ID = Integer.valueOf(wordsArray[0]);
//                    Optional<Vertex> v = g.V().has("id", v1ID).tryNext();
//                    Vertex v1 = v.orElseGet(() -> graph.addVertex("id", v1ID));
//                    Integer v2ID = Integer.valueOf(wordsArray[1]);
//                    v = g.V().has("id", v2ID).tryNext();
//                    Vertex v2 = v.orElseGet(() -> graph.addVertex("id", v2ID));
//                    nodes.add(v1ID);
//                    nodes.add(v2ID);
//                    v1.addEdge("co-author", v2);
//                }
//            }
//            buf.close();
//            g.close();
//        }catch(Exception e){
//            e.printStackTrace();
//        }
//        return graph;
//    }
//
//
//    private static Graph generateGraph(int size) {
//        Graph graph = TinkerGraph.open();
//        GraphTraversalSource g = graph.traversal();
//        Random rng = new Random(0);
//        List<Long> vIds = new ArrayList<>();
//        for (long i=0; i<size; i++) {
//            graph.addVertex();
//            vIds.add(i);
//        }
//        for (long i = 0; i < size; i++) {
//            int numOfNeighbor = (int) (rng.nextGaussian() * 2 + 5);
//            Vertex v1 = g.V().hasId(i).next();
//            for (int j=0; j<numOfNeighbor; j++) {
//                Random rand = new Random();
//                long randomNeighbor = vIds.get(rand.nextInt(vIds.size()));
//                Vertex v2 = g.V().hasId(randomNeighbor).next();
//                v1.addEdge("s", v2);
//            }
//        }
//        return graph;
//    }
//
//
//
//
//    // get neighbors -> coreness -> coreRank -> add property to node
//    public static void main(String[] args) throws IOException {
//        int size = 50000;
//        // todo: create toy graph
//        Graph g = generateGraph(size);
//        // there should be two SBAs, first: get neighbors, second: coreness (streamoutput) -> coreRank (pipeline) -> addProperty (streaminput)
//        // first one is a getNeighbor and this is the same for all different scheduling
//        GetNeighbors neighbor = new GetNeighbors(g);
//        neighbor.run();
//        Map<Integer, Set<Integer>> nbs = neighbor.getMaterializedOutput();
//
//        // sequential
//        long startTime = System.currentTimeMillis();
//        Coreness coreness1 = new Coreness(g, nbs);
//        coreness1.run();
//        List<Pair<Integer, Long>> corenessResult = coreness1.getMaterializedOutput();
//        long corenessTime = System.currentTimeMillis();
//        System.out.println("coreness costs: " + (corenessTime - startTime));
//        CoreRank corerank1 = new CoreRank(nbs, corenessResult);
//        List<Pair<Integer, Integer>> rankResult = corerank1.getMaterializedOutput();
//        corerank1.run();
//        long coreRankTime = System.currentTimeMillis();
//        System.out.println("corerank costs: " + (coreRankTime - corenessTime));
//        System.out.println(rankResult.size());
//        AddNodeProperty<Integer> nodeProperty = new AddNodeProperty<Integer>("coreRank", g, rankResult);
//        nodeProperty.run();
//        System.out.println("add node property costs: " + (System.currentTimeMillis() - coreRankTime));
//        System.out.println(String.format("total cost for %d nodes is %d ms", 10000, (System.currentTimeMillis() - startTime)));
//        System.out.println("finish");
//
//
//        // pipeline
//        List<ExecutorMetaData> executors = new ArrayList<>();
//        List<Integer> assignment = new ArrayList<>();
//        ExecutorMetaData coreness = new CorenessMeta(nbs, PipelineMode.streamoutput);
//        ExecutorMetaData corerank = new CoreRankMeta(nbs, PipelineMode.pipeline);
//        ExecutorMetaData addProperty = new NodePropertyMeta("coreRank", "Integer", g, PipelineMode.streaminput);
//        executors.add(coreness);
//        executors.add(corerank);
//        executors.add(addProperty);
//        assignment.add(1);
//        assignment.add(1);
//        assignment.add(1);
//        startTime = System.currentTimeMillis();
//        Object result = singleBatchAssign(executors, assignment, g, true);
//        System.out.println(String.format("total cost for %d nodes is %d ms", size, (System.currentTimeMillis() - startTime)));
//
//        // sequential execution
////        Graph g = TinkerFactory.createTheCrew();
////        GetNeighbors neighbor = new GetNeighbors(g);
////        neighbor.run();
////        Map<Integer, Set<Integer>> nbs = neighbor.getResult();
//
//    }
//
//
//
//}

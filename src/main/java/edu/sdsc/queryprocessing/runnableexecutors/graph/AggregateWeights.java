//package edu.sdsc.queryprocessing.depreciatedrunnableexecutors.graph;
//
//import edu.sdsc.datatype.execution.GraphElement;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.baserunnable.AwesomeStreamInputRunnable;
//
//import java.util.*;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.stream.Stream;
//
//// no parallel execution, so no latch
//public class AggregateWeights extends AwesomeStreamInputRunnable<GraphElement, List<GraphElement>> {
//
//    public AggregateWeights(PipelineMode mode) {
//        super(mode);
//    }
//
//
//    // stream input
//    public AggregateWeights(Stream<GraphElement> input) {
//        super(input);
//    }
//
//    // block
//    public AggregateWeights(List<GraphElement> input) {
//        super(input);
//    }
//
////    @Override
////    public List<GraphElement> getResult() {
////        return mergedGraphElement;
////    }
//
//    @Override
//    public void executeStreamInput() {
//        System.out.println("Aggregate Weights starts at: " +  System.currentTimeMillis());
//        Stream<GraphElement> input = getStreamInput();
//
//        try{
//            // for each document's graph element, add to hash map
//            HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
//            while (true) {
//                // get peek, not remove it, so other consumers will reach here too
//                // only need to decide if the input is empty, does not need a EOF
//                if (producers.getCount() <= 0) {
//                    System.out.println("it works");
//                    break;
//                }
//                else {
//                    List<GraphElement> graphData = input.take();
//                    if (graphData.equals(Collections.EMPTY_LIST)) {
//                        // when reach an empty list, it means all the input from one producer has been processed
//                        producers.countDown();
//                        if (producers.getCount() <= 0) {
//                            // when all producers's output have been processed, need to add an empty list back to the input queue
//                            //   so that other threads will reach it and count down and will not be blocked.
//                            input.put(Collections.emptyList());
//                            break;
//                        }
//                    }
//                    else {
//                        mergeExecutor(graphData, mergedGraph);
//                    }
//                }
//            }
//            List<GraphElement> mergedGraphElement = new ArrayList<>(mergedGraph.values());
//            System.out.println("Aggregate Weights countdown: " + getLatch().getCount() + " at " + System.currentTimeMillis());
//            setMaterializedOutput(mergedGraphElement);
//            countDown();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void executeBlocking() {
////        long start = System.currentTimeMillis();
//        List<GraphElement> materializedInput = getMaterializedInput();
//        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
//        mergeExecutor(materializedInput, mergedGraph);
//        List<GraphElement> mergedGraphElement = new ArrayList<>(mergedGraph.values());
////        countDown();
//        System.out.println("merged graph size: " + mergedGraphElement.size());
//        setMaterializedOutput(mergedGraphElement);
////        System.out.println(String.format("merge graph to get weights: %d ms",  System.currentTimeMillis() - start));
//    }
//
//    private void mergeExecutor(List<GraphElement> graphData, HashMap<GraphElement, GraphElement> mergedGraph) {
//        graphData.forEach(i -> {if (mergedGraph.containsKey(i)) {
//            int count = (Integer) mergedGraph.get(i).getProperty("count") + (Integer) i.getProperty("count");
//            mergedGraph.get(i).addProperty("count", count);
//        }
//        else {
//            mergedGraph.put(i, i); } });
//    }
//}

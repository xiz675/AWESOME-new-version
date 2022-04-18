//package edu.sdsc.queryprocessing.runnableexecutors.graph;
//
//import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
//import edu.sdsc.utils.Pair;
//
//import java.util.*;
//import java.util.concurrent.LinkedBlockingQueue;
//
//// for this operator, there can only be one producer, so do not need a semphor for producers.
//// it is not easy to be parallel
//public class CoreRank extends AwesomePipelineRunnable<Pair<Integer, Long>, Pair<Integer, Integer>> {
//    private Integer unitSize = 200;
//    private Map<Integer, Set<Integer>> neighbors;
//
//    // pipeline
//    public CoreRank(Map<Integer, Set<Integer>> nb, LinkedBlockingQueue<List<Pair<Integer, Long>>> input, LinkedBlockingQueue<List<Pair<Integer, Integer>>> result) {
//        super(input, result);
//        neighbors = nb;
//    }
//
//    // block
//    public CoreRank(Map<Integer, Set<Integer>> nb, List<Pair<Integer, Long>> input) {
//        super(input);
//        neighbors = nb;
//    }
//
//
//    @Override
//    public void executeStreamInput() {
//
//    }
//
//    @Override
//    public void executeStreamOutput() {
//
//    }
//
//    @Override
//    public void executePipeline() {
//        LinkedBlockingQueue<List<Pair<Integer, Long>>> streamInput = getStreamInput();
//        LinkedBlockingQueue<List<Pair<Integer, Integer>>> streamResult = getStreamResult();
//        // needs to keep track of coreRank value, and also for each node, what are the neighbors left
//        Map<Integer, Integer> nodeCoreRank = new HashMap<>();
//        Map<Integer, Set<Integer>> nodeProcessedNeighbor = new HashMap<>();
//        // for each neighbor
//        List<Pair<Integer, Integer>> coreRank = new ArrayList<>();
//        // for the coreness of one node, add it to all neighbors', and check if the neighbors have finished
//        try {
//            while(true) {
//                // modified a little, it is not after processing one, add one, cause the size of outcomes may be much fewer
//                // than the size of input
//                List<Pair<Integer, Long>> corenessList = streamInput.take();
//                if (corenessList.equals(Collections.EMPTY_LIST)) {
//                    // when finish the operator, needs to add EOF
////                    for (Pair<Integer, Integer> i : coreRank) {
////                        System.out.println("coreRank" + i.first + ": " + i.second);
////                    }
//                    streamResult.put(coreRank);
//                    streamResult.put(Collections.emptyList());
//                    break;
//                }
//                if (coreRank.size() >= unitSize) {
//                    streamResult.put(coreRank);
//                    coreRank = new ArrayList<>();
//                }
//                for (Pair<Integer, Long> i : corenessList) {
//                    Integer vID = i.first;
//                    Integer core = i.second.intValue();
//                    // get neighbors of the node with coreness
//                    Set<Integer> neighbor = neighbors.get(vID);
//                    // List<Integer> neighbor = g.traversal().V().hasId(vID).out().map(k -> (Integer) k.get().id()).toList();
//                    // for neighbor, add to their corerank and check if the neighbor finishes
//                    for (Integer j : neighbor) {
//                        Integer nbCoreRank = core;
//                        if (nodeCoreRank.containsKey(j)) {
//                            nbCoreRank = nodeCoreRank.get(j) + core;
//                            nodeCoreRank.put(j, nbCoreRank);
//                        }
//                        else {
//                            nodeCoreRank.put(j, nbCoreRank);
//                        }
//                        // for neighbors, check if it has finished all
//                        if (nodeProcessedNeighbor.containsKey(j)) {
//                            Set<Integer> neighborOfNb = nodeProcessedNeighbor.get(j);
//                            neighborOfNb.add(vID);
//                            // when the coreRank of the neighbor is ready, add to result
//                            if (neighborOfNb.size() == neighbors.get(j).size()) {
//                                coreRank.add(new Pair<>(j, nbCoreRank));
//                            }
//                        }
//                        else {
//                            Set<Integer> neighborOfNb = new HashSet<>();
//                            neighborOfNb.add(vID);
//                            nodeProcessedNeighbor.put(j, neighborOfNb);
//                        }
//                    }
//                }
//            }
//            System.out.println("CoreRank countdown: " + (getLatch().getCount() - 1) + " at " + System.currentTimeMillis());
//            countDown();
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // give coreness of all nodes and neighbor map, get corerank values of all nodes
//    @Override
//    public void executeBlocking() {
//        List<Pair<Integer, Long>> coreness = getMaterializedInput();
//        List<Pair<Integer, Integer>> coreRank = new ArrayList<>();
//        Map<Integer, Integer> nodeCoreRank = new HashMap<>();
//        for (Pair<Integer, Long> core : coreness) {
//            int vID = core.first;
//            int coreValue = core.second.intValue();
//            Set<Integer> nbs = neighbors.get(vID);
//            for (Integer nb : nbs) {
//                if (nodeCoreRank.containsKey(nb)) {
//                    nodeCoreRank.put(nb, nodeCoreRank.get(nb)+coreValue);
//                }
//                else {
//                    nodeCoreRank.put(nb, coreValue);
//                }
//            }
//        }
//        for (Integer nID : nodeCoreRank.keySet()) {
//            coreRank.add(new Pair<>(nID, nodeCoreRank.get(nID)));
//        }
//        setMaterializedOutput(coreRank);
//    }
//}

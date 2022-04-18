//package edu.sdsc.queryprocessing.runnableexecutors;
//import edu.sdsc.datatype.execution.Document;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
//import scala.Int;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class AddOne extends AwesomePipelineRunnable<Integer, Integer> {
//    // this has four different Runnable based on the execution mode
//    // based on the input type, there are two types
//
//    final private Integer unitSize = 50;
//
//    public AddOne(PipelineMode m) {
//        super(m);
//    }
//
//    // stream in constructor
//    public AddOne(LinkedBlockingQueue<List<Integer>> data) {
//        super(data);
//    }
//    // stream out constructor
//    public AddOne(List<Integer> data, LinkedBlockingQueue<List<Integer>> result) {
//        super(data, result);
//    }
//
//    // pipeline constructor:
//    public AddOne(LinkedBlockingQueue<List<Integer>> data, LinkedBlockingQueue<List<Integer>> result) {
//        super(data, result);
//    }
//
//    // blocking constructor:
//    public AddOne(List<Integer> input, List<Integer> result) {
//        super(input, result);
//    }
//
//
////    // for queue output, the queue is passed to constructor
////    public List<Integer> getMaterializedResult() {
////        Mode executionMode = getExecutionMode();
////        assert (executionMode.equals(Mode.block) || executionMode.equals(Mode.streaminput));
////        return getMaterializedResult();
////    }
//
//
//    // if result is a List, not add EOF
//    public void executeStreamInput() {
//        LinkedBlockingQueue<List<Integer>> input = getStreamInput();
//        List<Integer> result = getMaterializedResult();
//        try {
//            while (!isCancel()) {
//                // get peek, not remove it, so other consumers will reach here too
//                List<Integer> t = input.peek();
//                if (t == null || t.size()==0) {
//                    setCancel(true);
//                    countDown();
//                }
//                else {
//                    input.take();
//                    for (Integer i: t) {
//                        Thread.sleep(10);
//                        result.add(i + 1);
//                    }
//
////                    System.out.println(Thread.currentThread().getName() + ": consume " + t);
//                }
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    // result has an EOF
//    public void executeStreamOutput() {
//        List<Integer> materializedInput = getMaterializedInput();
//        LinkedBlockingQueue<List<Integer>> streamResult = getStreamResult();
//        try {
//            int count = unitSize;
//            List<Integer> temp = new ArrayList<>();
//            for (Integer i:materializedInput) {
//                if (count == 0) {
//                    streamResult.put(temp);
//                    temp = new ArrayList<>();
//                    count = unitSize;
//                }
//                Thread.sleep(10);
//                temp.add(i+1);
//                count-=1;
//            }
//            if (temp.size() != 0) {
//                streamResult.put(temp);
//            }
//            streamResult.put(new ArrayList<>());
//            countDown();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void executePipeline() {
//        LinkedBlockingQueue<List<Integer>> input = getStreamInput();
//        LinkedBlockingQueue<List<Integer>> streamResult = getStreamResult();
//        try {
//            while (!isCancel()) {
//                // get peek, not remove it, so other consumers will reach here too
//                List<Integer> t = input.peek();
//                if (t==null || t.size()==0) {
//                    // result should have an EOF
//                    streamResult.add(new ArrayList<>());
//                    setCancel(true);
//                    countDown();
//                }
//                else {
//                    List<Integer> temp = new ArrayList<>();
//                    input.take();
//                    for (int i:t) {
//                        Thread.sleep(10);
//                        temp.add(i+1);
//                    }
//                    streamResult.put(temp);
////                    System.out.println(Thread.currentThread().getName() + ": consume " + t);
//                }
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void executeBlocking() {
//        List<Integer> materializedInput = getMaterializedInput();
//        List<Integer> materializedResult = getMaterializedResult();
//        try {
//            for (Integer i:materializedInput) {
//                Thread.sleep(10);
//                materializedResult.add(i+1);
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        countDown();
//    }
//}

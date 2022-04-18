//package edu.sdsc.queryprocessing.runnableexecutors;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class AddTwo extends AwesomePipelineRunnable<Integer, Integer> {
//    final private Integer unitSize = 50;
//    // if the input is a stream, then it is possible that the input is from several producers
//    private Integer producers = 1;
//
//    public AddTwo(PipelineMode m) {
//        super(m);
//    }
//
//    // stream in constructor
//    public AddTwo(LinkedBlockingQueue<List<Integer>> data, CountDownLatch latch, Integer... producers) {
//        super(data);
//        this.setLatch(latch);
//        if (producers.length > 0) {
//            this.producers = producers[0];
//        }
//    }
//    // stream out constructor
//    public AddTwo(List<Integer> data, LinkedBlockingQueue<List<Integer>> result, CountDownLatch latch) {
//        super(data, result);
//        this.setLatch(latch);
//    }
//
//    // pipeline constructor:
//    public AddTwo(LinkedBlockingQueue<List<Integer>> data, LinkedBlockingQueue<List<Integer>> result, CountDownLatch latch, Integer... producers) {
//        super(data, result);
//        this.setLatch(latch);
//        if (producers.length > 0) {
//            this.producers = producers[0];
//        }
//    }
//
//    // blocking constructor:
//    public AddTwo(List<Integer> input, List<Integer> result, CountDownLatch latch) {
//        super(input, result);
//        this.setLatch(latch);
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
//                // get peek, not remove it, so other producers will reach here too
//                List<Integer> t = input.peek();
//                if (t != null && t.size()==0) {
//                    producers = producers - 1;
//                    if (producers==0) {
//                        setCancel(true);
//                        countDown();
//                    }
//                    else {
//                        // continue executing the later
//                        input.take();
//                    }
//                }
//                else if (t!=null){
//                    input.take();
//                    for (Integer i: t) {
//                        Thread.sleep(2);
//                        result.add(i + 2);
//                    }
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
//                Thread.sleep(2);
//                temp.add(i+2);
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
//                // get peek, not remove it, so other producers will reach here too
//                List<Integer> t = input.peek();
//                if (t!=null && t.size()==0) {
//                    producers -= 1;
//                    if (producers == 0) {
//                        // result should have an EOF
//                        streamResult.add(new ArrayList<>());
//                        setCancel(true);
//                        countDown();
//                    }
//                    else {
//                        input.take();
//                    }
//                }
//                else if (t!=null) {
//                    List<Integer> temp = new ArrayList<>();
//                    input.take();
//                    for (int i:t) {
//                        Thread.sleep(2);
//                        temp.add(i+2);
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
//                Thread.sleep(2);
//                materializedResult.add(i+2);
//            }
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        countDown();
//    }
//}

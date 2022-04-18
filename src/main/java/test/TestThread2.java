package test;

import edu.sdsc.datatype.execution.Document;
import scala.Int;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class TestThread2 implements Runnable{
    private List<Integer> materializedInput;
    private LinkedBlockingQueue<List<Integer>> streamInput;
    private List<Integer> materializedResult;
    private LinkedBlockingQueue<List<Integer>> streamResult;
    private String mode;
    private CountDownLatch latch;
    private Integer unitSize = 500;
    private CountDownLatch producer = new CountDownLatch(1);
    private boolean cancel = false;

    // stream in constructor
    public TestThread2(LinkedBlockingQueue<List<Integer>> data, List<Integer> result, CountDownLatch... producers) {
        mode = "in";
        streamInput = data;
        if (producers.length > 0) {
            setProducer(producers[0]);
        }
    }

    // stream out constructor
    public TestThread2(List<Integer> data, LinkedBlockingQueue<List<Integer>> result) {
        mode = "out";
        materializedInput = data;
        streamResult = result;
    }

    // pipeline constructor
    public TestThread2(LinkedBlockingQueue<List<Integer>> data, LinkedBlockingQueue<List<Integer>> result, CountDownLatch... producers) {
        mode = "pipeline";
        streamInput = data;
        streamResult = result;
        if (producers.length > 0) {
            setProducer(producers[0]);
        }
    }

    // blocking constructor
    public TestThread2(List<Integer> data, List<Integer> result) {
        mode = "block";
        materializedInput = data;
        materializedResult = result;
    }

    public void setProducer(CountDownLatch producer) {
        this.producer = producer;
    }

    public CountDownLatch getProducer() {
        return producer;
    }

    public void setMaterializedInput(List<Integer> materializedInput) {
        this.materializedInput = materializedInput;
    }

    public void setMaterializedResult(List<Integer> materializedResult) {
        this.materializedResult = materializedResult;
    }

    public void setStreamInput(LinkedBlockingQueue<List<Integer>> streamInput) {
        this.streamInput = streamInput;
    }

    public void setStreamResult(LinkedBlockingQueue<List<Integer>> streamResult) {
        this.streamResult = streamResult;
    }

    public List<Integer> getMaterializedInput() {
        return materializedInput;
    }

    public LinkedBlockingQueue<List<Integer>> getStreamInput() {
        return streamInput;
    }

    public LinkedBlockingQueue<List<Integer>> getStreamResult() {
        return streamResult;
    }

    public List<Integer> getMaterializedResult() {
        return materializedResult;
    }

    public CountDownLatch getLatch() {
        return latch;
    }

    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void countDown() {
        latch.countDown();
    }

//    public boolean isCancel() {
//        return this.cancel;
//    }

//    public void setCancel(boolean cancel) {
//        this.cancel = cancel;
//    }

    private void executeStreamOutput() {
        List<Integer> materializedInput = getMaterializedInput();
        LinkedBlockingQueue<List<Integer>> streamResult = getStreamResult();
        try {
            int count = unitSize;
            List<Integer> temp = new ArrayList<>();
            for (Integer t:materializedInput) {
                if (count==0) {
                    streamResult.put(temp);
                    temp = new ArrayList<>();
                    count = unitSize;
                }
                // t.setTokens(Arrays.asList(t.text.split(splitter)));
                temp.add(t + 100);
                count-=1;
            }
            streamResult.put(Collections.emptyList());
            countDown();
            System.out.println("TestThread2 countdown: " + getLatch().getCount());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void executePipeline() {
        LinkedBlockingQueue<List<Integer>> input = getStreamInput();
        LinkedBlockingQueue<List<Integer>> streamResult = getStreamResult();
        CountDownLatch producers = getProducer();
        boolean cancel = false;
        try {
            while (!cancel) {
                // if the producer has count larger than 0, it means that the consumers can keep consuming
                if (producers.getCount() <= 0) {
                    cancel = true;
                    streamResult.put(Collections.emptyList());
                    System.out.println("it works");
                }
                else {
                    List<Integer> t = input.take();
                    if (t.equals(Collections.EMPTY_LIST)) {
                        // when reach an empty list, it means all the input from one producer has been processed
                        producers.countDown();
                        if (producers.getCount() <= 0) {
                            // when all producers's output have been processed, need to add an empty list back to the input queue
                            //   so that other threads will reach it and count down and will not be blocked.
                            input.put(Collections.emptyList());
                            streamResult.put(Collections.emptyList());
                            break;
                        }
                    }
                    else {
                        List<Integer> temp = new ArrayList<>();
                        for (Integer i:t) {
                            temp.add(i + 100);
                        }
                        streamResult.put(temp);
                        Thread.sleep(20);
                    }
                }
            }
            System.out.println("TestThread2 countdown: " + getLatch().getCount() + " at " + System.currentTimeMillis());
            countDown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void run() {
        switch (mode) {
            case "out": {
                executeStreamOutput();
                break;
            }
            case "pipeline": {
                executePipeline();
                break;
            }
        }
    }
}

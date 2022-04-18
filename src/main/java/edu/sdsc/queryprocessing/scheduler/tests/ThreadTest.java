package edu.sdsc.queryprocessing.scheduler.tests;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;


public class ThreadTest {
  
  static class Pair<U, V> {
    U first;
    V second;

    public Pair(U first, V second) {
      this.first = first;
      this.second = second;
    }
  }

  static class ListAddOneOp extends Thread {
    private final List<Integer> data;
    private final List<Integer> result;
    private final CountDownLatch latch;

    public ListAddOneOp(List<Integer> data, CountDownLatch latch) {
      this.data = data;
      this.result = new ArrayList<>(data.size());
      this.latch = latch;
    }


    @Override
    public void run() {
      for (Integer datum : data) {
        this.result.add(datum + 1);
      }
      System.out.println(currentThread().getName() + ": Finish this task!");
      latch.countDown();
    }
  }

  public static List<Integer> makeData(int size) {
    List<Integer> result = new ArrayList<>(size);
    for (int i=0; i<size; ++i) {
      result.add(i);
    }
    return result;
  }

  public static void main(String[] args) throws InterruptedException {
    List<Integer> data = makeData(8_000_000);
    int numThreads = 8;
    CountDownLatch latch = new CountDownLatch(numThreads);
    List<ListAddOneOp> tasks = new ArrayList<>();
    for (int i=0; i<numThreads; ++i) {
      ListAddOneOp task = new ListAddOneOp(data.subList(i, i + 800_000), latch);
      task.start();
      tasks.add(task);
    }
    latch.await();
    System.out.println("Finish all tasks!");
    List<Integer> result = new ArrayList<>();
    for (ListAddOneOp task : tasks) {
      result.addAll(task.result);
    }
    System.out.println(result.subList(50, 80));
  }
}

package test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

public class TestThreadMain {
    public static void main(String[] args) throws InterruptedException {
        // 1 thread, 1 thread, and 8 threads
        CountDownLatch latch = new CountDownLatch(2);
        List<Integer> data = makeData(1000000);
        LinkedBlockingQueue<List<Integer>> output1 = new LinkedBlockingQueue<>();
        TestThread p1 = new TestThread(data, output1);
        p1.setLatch(latch);
        Thread t1 = new Thread(p1);
        t1.start();
        LinkedBlockingQueue<List<Integer>> output2 = new LinkedBlockingQueue<>();
        TestThread p2 = new TestThread(output1, output2);
        p2.setLatch(latch);
        Thread t2 = new Thread(p2);
        t2.start();
//        LinkedBlockingQueue<List<Integer>> output3 = new LinkedBlockingQueue<>();
//        // if an operation has multiple threads, should let them share a same latch so that they can synchronize
//        CountDownLatch producer = new CountDownLatch(1);
//        for (int i=0; i<8; i++) {
//            TestThread2 p3 = new TestThread2(output2, output3, producer);
//            p3.setLatch(latch);
//            Thread t3 = new Thread(p3);
//            t3.start();
//        }
        latch.await();
        System.out.println(latch.getCount());
        System.out.println(output1.size());
        System.out.println(output2.size());
//        System.out.println(output3.size());
    }

    private static List<Integer> makeData(int size) {
        List<Integer> result = new ArrayList<>(size);
        for (int i=0; i<size; ++i) {
            result.add(i);
        }
        return result;
    }

}

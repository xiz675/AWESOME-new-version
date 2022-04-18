package edu.sdsc.queryprocessing.scheduler.tests;

import java.util.concurrent.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class PipelineTest {

    public static void main(String[] args) throws InterruptedException {
        LinkedBlockingQueue<Integer> concurrentLinkedQueue = new LinkedBlockingQueue<>();
        Thread pT = new Thread(new Producer("p1", concurrentLinkedQueue));
        Thread cT = new Thread(new Consumer("c1", concurrentLinkedQueue));

        pT.start();
        cT.start();


//        ExecutorService executorService = Executors.newFixedThreadPool(6);
//        executorService.submit(new Producer("producer1"));
//        executorService.submit(new Producer("producer2"));
//        executorService.submit(new Producer("producer3"));
////        executorService.submit(new Consumer("consumer1"));
////        executorService.submit(new Consumer("consumer2"));
////        executorService.submit(new Consumer("consumer3"));
//        executorService.shutdown();
    }

    static class Producer implements Runnable {
        private String name;
        private LinkedBlockingQueue<Integer> queue;

        public Producer(String name, LinkedBlockingQueue<Integer> queue) {
            this.name = name;
            this.queue = queue;
        }

        public void run() {
            try {
                for (int i = 1; i < 5; ++i) {
                    System.out.println(name + "  生产： " + i);
                    //concurrentLinkedQueue.add(i);
                    queue.put(i);
                    Thread.sleep(2000); //模拟慢速的生产，产生阻塞的效果
                }
                queue.put(-1);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
        }
    }

    static class Consumer implements Runnable {
        private String name;
        volatile boolean cancel = false;
        private LinkedBlockingQueue<Integer> queue;

        public Consumer(String name, LinkedBlockingQueue<Integer> queue) {
            this.name = name;
            this.queue = queue;
        }

        public void cancel() {
            cancel = true;
        }

        public void run() {
            while (!cancel) {
                if (!queue.isEmpty()) {
                    try {
                        Integer value = queue.take();
                        if (value==-1) {
                            cancel = true;
                        }
                        System.out.println(name + " 消费： " + value);
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return ;
                    }
                }
            }
        }
    }
}

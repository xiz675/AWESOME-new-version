package edu.sdsc.queryprocessing.executor.execution.executionunit.tests;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.IntStream;

class Sum implements Runnable {
    private int[] list;
    private static CopyOnWriteArrayList<Integer> rsult;

    public Sum(int[] l, CopyOnWriteArrayList rsult) {
        this.list = l;
        this.rsult = rsult;
    }
    @Override
    public void run() {
        int sum = IntStream.of(list).sum();
        rsult.add(sum);
        System.out.println(rsult);
    }
}

class Task implements Callable<Integer> {
    private int[] list;


    public Task(int[] l) {
        this.list = l;
    }

    @Override
    public Integer call() throws Exception {
        int sum = IntStream.of(list).sum();
        System.out.println(list);
        System.out.println(Thread.currentThread().getName() + " run time: " + System.currentTimeMillis());
        return sum;
    }
}

public class Test {
    static CopyOnWriteArrayList<Integer> rsult = new CopyOnWriteArrayList<>();

    public void parallelCall(List<Integer> x) throws ExecutionException, InterruptedException {
        ExecutorService es = Executors.newFixedThreadPool(4);
        int[] num = new int[20];
        int pointer = 0;
        List<Future<Integer>> rsult = new ArrayList<>();
        boolean submitted = false;
        for (Integer integer : x) {
            num[pointer] = integer;
            submitted = false;
            if (pointer == num.length - 1) {
                pointer = -1;
                Future<Integer> f =  es.submit(new Task(num));
                rsult.add(f);
                submitted = true;
                num = new int[20];
            }
            pointer = pointer + 1;
        }
        if (!submitted) {
            Future<Integer> f =  es.submit(new Task(num));
            rsult.add(f);
        }
        es.shutdown();
        int sum = 0;
        for(Future<Integer> f : rsult) {
            sum += f.get();
        }
        System.out.println(sum);
    }




    public static void main(String[] args) throws ExecutionException, InterruptedException {

        ArrayList<Integer> x = new ArrayList<>();
        for (int i = 0; i < 96; i++) {
            x.add(i);
        }
        ExecutorService es = Executors.newFixedThreadPool(4);
        int[] num = new int[20];
        int pointer = 0;
        boolean submitted = false;
        for (Integer integer : x) {
            num[pointer] = integer;
            submitted = false;
            if (pointer == num.length - 1) {
                pointer = -1;
                int[] finalNum = num;
                es.execute(new Runnable() {
                    @Override
                    public void run() {
                        int sum = IntStream.of(finalNum).sum();
                        rsult.add(sum);
                    }
                });
                submitted = true;
                num = new int[20];
            }
            pointer = pointer + 1;
        }
        if (!submitted) {
            int[] finalNum = num;
            es.execute(() -> {int sum = IntStream.of(finalNum).sum(); rsult.add(sum);});
        }
        es.shutdown();
        System.out.println(rsult);
        System.out.println(96/10);




//        Stream<Integer> infiniteStream = Stream.iterate(0, i -> i + 2);
//
//// when
//        List<Integer> collect = infiniteStream
//                .limit(20)
//                .collect(Collectors.toList());
//        System.out.println(collect);
    }
}






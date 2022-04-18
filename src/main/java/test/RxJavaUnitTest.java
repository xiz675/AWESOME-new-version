package test;

import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import io.reactivex.rxjava3.parallel.ParallelFlowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.Assert.assertTrue;


public class RxJavaUnitTest {
    String result="";

    private ParallelFlowable<Integer> addOne(ParallelFlowable<Integer> input) {
        return input.map(i-> i+1);
    }

    private ParallelFlowable<Integer> decreaseOne(ParallelFlowable<Integer> input) {
        return input.map(i -> i-1);
    }

    private Observable<Integer> addOne(Observable<Integer> input) {
        return input.map(i-> i+1);
    }

    private List<Integer> decreaseOneMaterialize(ParallelFlowable<Integer> input) {
        return input.map(i -> i-1).sequential().toList().blockingGet();
    }

    private Integer materialize(Flowable<Integer> input) {
        return input.reduce(Integer::sum).blockingGet();
    }

    private ParallelFlowable<Integer> createStream() {
        try {
            TimeUnit.MILLISECONDS.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
//        return Flowable.range(1, 100);
        return Flowable.range(1, 100).replay().autoConnect().subscribeOn(Schedulers.computation()).parallel();
    }



//    public static void main(String[] args) {
//        final ExecutorService executor = Executors.newFixedThreadPool(8);
//        Flowable<Integer> dataStream1 = Flowable.range(1, 4).replay().autoConnect();
//        Flowable<Integer> dataStream2 = dataStream1.subscribeOn(Schedulers.computation()).map(i -> {
//            System.out.println(i + " a " + Thread.currentThread().getName());
//            Thread.sleep(3000);
//            return i+1;});
//        dataStream2.replay().autoConnect().observeOn(Schedulers.computation()).map(i -> {
//            System.out.println(i + " b " + Thread.currentThread().getName());
//            Thread.sleep(1000);
//            return i + 1;
//        }).reduce(Integer::sum).blockingGet();}
//        System.out.println(x);
//        System.out.println(x);
//        final ExecutorService executor = Executors.newFixedThreadPool(8);
//        ParallelFlowable<Integer> dataStream1 = Flowable.range(1, 4).replay().autoConnect().parallel();
//        dataStream1.runOn(Schedulers.from(executor)).map(i -> {
//            System.out.println(i + " a " + Thread.currentThread().getName());
//            Thread.sleep(3000);
//            return i+1;}).sequential().subscribe();
//        dataStream1.runOn(Schedulers.from(executor)).map(i -> {
//            System.out.println(i + " b " + Thread.currentThread().getName());
//            Thread.sleep(1000);
//            return i+1;}).sequential().subscribe();



    @Test
    public void testFunc() {
//        ConnectableObservable<Integer> dataStream = Observable.just(1, 2, 3, 4, 5).publish();
//        Flowable<Integer> dataStream = Flowable.range(1, 10).replay().autoConnect().subscribeOn(Schedulers.computation());
        Long startTime = System.currentTimeMillis();
        // parallel execution.
        ParallelFlowable<Integer> dataStream = Flowable.range(1, 4).replay().autoConnect().parallel().runOn(Schedulers.computation());
        Flowable<Integer> data1 = dataStream.map(i -> {
            System.out.println(i + " a " + Thread.currentThread().getName()); Thread.sleep(3000); return i+1;}).sequential();
        Flowable<Integer> data2 = data1.parallel().runOn(Schedulers.computation())
                .map(i -> { System.out.println(i + " b " + Thread.currentThread().getName()); Thread.sleep(1000); return i+1;})
                .sequential();
        System.out.println("prepare time: " + (System.currentTimeMillis() - startTime));
        materialize(data2);
        Long endTime = System.currentTimeMillis();
        System.out.println("parallel execution spent:" + (endTime-startTime));

        // pipeline execution
        startTime = System.currentTimeMillis();
        Flowable<Integer> dataStream2 = Flowable.range(1, 4).replay().autoConnect().subscribeOn(Schedulers.computation());
         Flowable<Integer> data12 = dataStream2.replay().autoConnect().map(i -> {
             System.out.println(i + " a " + Thread.currentThread().getName()); Thread.sleep(3000); return i+1;});
         Flowable<Integer> data22 = data12.replay().autoConnect().observeOn(Schedulers.computation()).map(i -> { System.out.println(i + " b " + Thread.currentThread().getName()); Thread.sleep(1000); return i+1;});
         System.out.println("prepare time: " + (System.currentTimeMillis() - startTime));
         materialize(data22);
         endTime = System.currentTimeMillis();
         System.out.println("pipeline execution spent:" + (endTime-startTime));

        startTime = System.currentTimeMillis();
        Flowable<Integer> dataStream3 = Flowable.range(1, 4).replay().autoConnect().subscribeOn(Schedulers.computation());
        Flowable<Integer> data13 = dataStream3.replay().autoConnect().map(i -> {
            System.out.println(i + " a " + Thread.currentThread().getName()); Thread.sleep(3000); return i+1;});
        Flowable<Integer> data23 = data13.replay().autoConnect().map(i -> { System.out.println(i + " b " + Thread.currentThread().getName()); Thread.sleep(1000); return i+1;});
        System.out.println("prepare time: " + (System.currentTimeMillis() - startTime));
        materialize(data23);
        endTime = System.currentTimeMillis();
        System.out.println("execution spent:" + (endTime-startTime));
    }

//        List<Integer> data4 = decreaseOneMaterialize(data2);
//        System.out.println(data4);
//        Flowable<Integer> data5 = decreaseOne(decreaseOne(data2));
//        List<Integer> data6 = decreaseOneMaterialize(data5);
//        System.out.println(data6);
//        System.out.println("subscribing A");
//
//        Observable<Integer> dataStream = Observable.range(1, 10).publish().autoConnect(3);
//        Observable<Integer> dataStream = Observable.range(1, 10).publish().autoConnect(2);
////        addOne(dataStream).subscribe(s -> System.out.println(s));
////        List<Integer> c = decreaseOne(dataStream);
//
//        Observable<Integer> a = addOne(dataStream);
//        Observable<Integer> b = addOne(a);
//        List<Integer> c = decreaseOne(b);
//        List<Integer> data = decreaseOne(dataStream);
////        TimeUnit.MILLISECONDS.sleep(200);
////        System.out.println("subscribing B");
////        List<Integer> data = dataStream.map(i -> i-1).toList().blockingGet();
////        dataStream.map(i -> i+1).subscribe(s -> System.out.println(s));
//
//
//        Observable<Integer> dataStream = Observable.range(1, 10).replay().autoConnect().subscribeOn(Schedulers.computation());
//        List<Integer> c = decreaseOne(dataStream);
//        Observable<Integer> a = addOne(dataStream);
////        Flowable<Integer> b = addOne(a);
////        List<Integer> c = decreaseOne(b);
//        a.subscribe(s -> System.out.println(s));
//        List<Integer> data = decreaseOne(dataStream);
//        addOne(dataStream).subscribe(s -> System.out.println(s));
//        List<Integer> data = decreaseOne(dataStream);
//        System.out.println(data);
//        System.out.println(c);


    // Simple subscription to a fix value
    public void returnAValue(){
        result = "";
        Observable<String> observer = Observable.just("Hello"); // provides data
        observer.subscribe(s -> result=s); // Callable as subscriber
        assertTrue(result.equals("Hello"));
    }

//    @Test
    public void shareForMultiSub() throws InterruptedException {
        ConnectableObservable<Long> dataStream = Observable.interval(100, TimeUnit.MILLISECONDS).take(5).publish();
        System.out.println("subscribing A");
        dataStream.subscribe(v -> System.out.println("A got " + v));
        TimeUnit.MILLISECONDS.sleep(200);
        System.out.println("subscribing B");
        dataStream.subscribe(v -> System.out.println("B got " + v));
        dataStream.connect();
        TimeUnit.SECONDS.sleep(1);
    }

    public Flowable<Integer> intervalSequence(int start, int step, int end)
    {
        return Flowable.generate(()->start,
                (s, emitter)->{
                    if (s + step > end ) {
                        emitter.onComplete();}
                    int next = s+step;
                    emitter.onNext(next);
                    return next;
                });
    }
//    public <T> Flowable<T> streamToObservable(int start, int step, int end, Stream<T> stream) {
//        return Flowable.generate(
//                () -> stream.iterator(),
//                (ite, emitter) -> {
//                    if (ite.hasNext()) {
//                        emitter.onNext(ite.next());
//                    } else {
//                        emitter.onComplete();
//                    }
//                }
//        );
//    }
//    @Test
    public void pageRangeTest() throws Exception {
        Flowable<Integer> from = intervalSequence(1, 5, 10);
        from.take(5).doOnNext(System.out::println).subscribe();
    }

}
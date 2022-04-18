//package edu.sdsc.queryprocessing.scheduler;
//
//import edu.sdsc.datatype.execution.Document;
//import edu.sdsc.datatype.execution.Mode;
////import edu.sdsc.queryprocessing.scheduler.tests.AddOnePipe;
//import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;
//import edu.sdsc.queryprocessing.executors.text.CreateDocumentsPhysical;
////import edu.sdsc.queryprocessingng.scheduler.tests.executors.text.CreateDocumentsTest;
//import edu.sdsc.queryprocessing.executors.text.FilterDocumentsPhysical;
//import edu.sdsc.queryprocessing.executors.text.SplitDocumentsPhysical;
//import edu.sdsc.queryprocessing.executors.baserunnable.AwesomePipelineRunnable;
//
//import java.io.BufferedReader;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingQueue;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum.AddOne;
//import static edu.sdsc.queryprocessing.scheduler.tests.ThreadTest.makeData;
///* test: given a input list, producer add 1 to each and then consumer add 1 to each */
//
//public class PipelineUtil {
//    // todo: when producer's input is empty, add a EOF to the queue and break the loop. When consumer reaches the EOF, cancel
////    private List<AwesomeRunnable<I, O1>> producer;
////    private List<AwesomeRunnable<O1, O2>> consumer;
////    private LinkedBlockingQueue<O2> result;
//
//     // can choose number of parallel threads and also if to use pipeline
//     // if pipeline, then there should be 2*numThreads threads
//
//    public static List<Integer> multiThreadRun(Integer numParallelThreads, boolean isPipeline) throws InterruptedException {
//        CountDownLatch latch;
//        List<Integer> data = makeData(10000);
//        Long startTime;
//        Long endTime;
//        int subListSize = data.size()/numParallelThreads;
//        if (isPipeline) {
//            // if pipeline, then the total number of threads should be numParallel * numOperators
//            latch = new CountDownLatch(numParallelThreads*2);
//            List<List<Integer>> results = new ArrayList<>();
//            startTime = System.currentTimeMillis();
//            // for operator with pipeline cap, it gets data from queue and writes to a queue;
//            for (int i=0; i<numParallelThreads; i++) {
//                List<Integer> input;
//                LinkedBlockingQueue<Integer> output1 = new LinkedBlockingQueue<>();
//                List<Integer> output2 = new ArrayList<>();
//                results.add(output2);
//                if (i==numParallelThreads-1) {
//                    input = data.subList(i*subListSize, data.size());
//                }
//                else {
//                    input = data.subList(i*subListSize, (i+1)*subListSize);
//                }
////                System.out.println("input size:" + input.size());
//                // stream output
//                AddOne task = new AddOne(input, output1, latch);
//                Thread p1 = new Thread(task);
//                // pipeline
//                Thread c1 = new Thread(new AddOne(output1, output2, latch));
//                p1.start();
//                c1.start();
//            }
//            latch.await();
//            List<Integer> result = results.stream().flatMap(List::stream).collect(Collectors.toList());
//            endTime = System.currentTimeMillis();
//            System.out.println(String.format("Parallel threads: %d PipelineUtil: %s costs: %d", numParallelThreads, isPipeline, endTime -startTime));
//            return result;
//        }
//        else {
//            // no pipeline, each operator executed in parallel. Call blocking method
//            List<List<Integer>> inputs = new ArrayList<>();
//            startTime = System.currentTimeMillis();
//            for (int i=0; i<numParallelThreads; i++) {
//                if (i==numParallelThreads-1) {
//                    inputs.add(data.subList(i*subListSize, data.size()));
//                }
//                else {
//                    inputs.add(data.subList(i*subListSize, (i+1)*subListSize));
//                }
//            }
//            List<List<Integer>> tempResult = parallelExecutionUnit(inputs, numParallelThreads, AddOne);
//            // new latch and new result list for the second operator
//            List<List<Integer>> results = parallelExecutionUnit(tempResult, numParallelThreads, AddOne);
//            List<Integer> result = results.stream().flatMap(List::stream).collect(Collectors.toList());
//            endTime = System.currentTimeMillis();
//            System.out.println(String.format("Parallel threads: %d Pipeline: %s costs: %d ms", numParallelThreads, isPipeline, endTime -startTime));
//            return result;
//        }
//    }
//
//    private static AwesomePipelineRunnable makeExecutor(ExecutorEnum executor) {
//        switch (executor) {
//            case AddOne:
//                return new AddOne(Mode.block);
//            case CreateDocumentsPhysical:
//                return new CreateDocumentsPhysical(Mode.block, "newsid", "newstext");
//            case SplitDocumentsPhysical:
//                return new SplitDocumentsPhysical(Mode.block, " ");
//            case FilterDocumentsPhysical:
//                return new FilterDocumentsPhysical(Mode.block, getStopWords("C:/Users/xw/IdeaProjects/awesome-new-version/stopwords.txt"));
//            default:
//                return new AddOne(Mode.block);
//        }
//    }
//
//
//    private static List<String> getStopWords(String path) {
//        List<String> words = new ArrayList<>();
//        try {
//            BufferedReader objReader = new BufferedReader(new FileReader(path));
//            while (true) {
//                String crtLine = null;
//                try {
//                    crtLine = objReader.readLine();
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//                if (crtLine == null) break;
//                words.add(crtLine);
//            }
//            return words;
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//
//
//    public static void main(String[] args) throws InterruptedException, IOException {
////        JsonObject db = LoadConfig.getConfig("newsDB");
////        String loc = "News";
////        String sqlStatement = "select id as newsid, news as newstext from usnewspaper where src = ' http://www.chicagotribune.com/' order by id limit 10000";
////        RDBMSUtils db_util = new RDBMSUtils(db, loc);
////        Connection fromCon = db_util.getConnection();
////        DSLContext create = DSL.using(fromCon, SQLDialect.POSTGRES);
////        // parallelism only
////        long startTime = System.currentTimeMillis();
//////        System.out.println("parallelism execution start time: " + System.currentTimeMillis());
////        Stream<AwesomeRecord> result = create.resultQuery(sqlStatement).fetchStream().map(AwesomeRecord::new);
////        List<AwesomeRecord> results = result.collect(Collectors.toList());
////        System.out.println("executionSQL time: " + (System.currentTimeMillis() - startTime));
//        int numThreads = 1;
//        // create documents by partition data
////        List<List<AwesomeRecord>> partitionedData = partitionData(results, numThreads);
//        // List<List<Document>> docs1 = parallelExecutionUnit(partitionedData, numThreads, ExecutorEnum.CreateDocumentsPhysical);
//
//        // for some operators, it can't be accelerate by data partition, so latch only fits when they are executed in pipeline with others
//        int numDocs = 10000;
//        List<Document> data = new ArrayList<>();
////        CountDownLatch latch = new CountDownLatch(1);
//        long startTime = System.currentTimeMillis();
////        CreateDocumentsPhysical docs = new CreateDocumentsPhysical(numDocs, data);
////        // no need to have thread
////        docs.executeBlocking();
//        long creationTime = System.currentTimeMillis();
//        System.out.println("create documents: " + (creationTime - startTime));
//        List<List<Document>> partitionedData = partitionData(data, numThreads);
//        List<List<Document>> docs1 = parallelExecutionUnit(partitionedData, numThreads, ExecutorEnum.SplitDocumentsPhysical);
//        long splitTime = System.currentTimeMillis();
//        System.out.println("split documents: " + (splitTime - startTime));
//        List<List<Document>> docs2 = parallelExecutionUnit(docs1, numThreads, ExecutorEnum.FilterDocumentsPhysical);
//        System.out.println("filter documents: " + (System.currentTimeMillis() - splitTime));
//        System.out.println("parallelism execution end time: " + (System.currentTimeMillis() - startTime));
////        for (List<Document> i : docs3) {
////            System.out.println(i.size());
////        }
//    }
//
//}

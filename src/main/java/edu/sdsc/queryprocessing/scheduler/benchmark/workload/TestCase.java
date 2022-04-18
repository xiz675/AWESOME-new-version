package edu.sdsc.queryprocessing.scheduler.benchmark.workload;

import edu.sdsc.datatype.execution.Document;
import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.sdsc.queryprocessing.runnableexecutors.text.FilterDocuments;
import edu.sdsc.queryprocessing.runnableexecutors.text.SplitDocuments;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;

public class TestCase {
    public static List<Document> createDocuments(Integer size) {
        List<Document> x = new ArrayList<>();
        String text = "";
        for (int i=0; i<500; i++) {
            text += "abc ";
        }
        for (int i=0; i<size; i++) {
            Document doc = new Document(i, text);
            x.add(doc);
        }
        return x;
    }

    private static List<List> partitionData(List input, int numThreads) {
        List<List> results = new ArrayList<>();
        int subListSize = input.size()/numThreads;
        for (int i=0; i<numThreads; i++) {
            if (i==numThreads-1) {
                results.add(input.subList(i*subListSize, input.size()));
            }
            else {
                results.add(input.subList(i*subListSize, (i+1)*subListSize));
            }
        }
        return results;
    }

    private static AwesomePipelineRunnable makeASingleParallelExecutor(String operator, List<String>... stopwords) {
        // todo: add all operators in the first case
        if (operator.equals("split")) {
            return new SplitDocuments(PipelineMode.block, " ");
        }
        else {
            assert operator.equals("filter");
            return new FilterDocuments(PipelineMode.block, stopwords[0]);
        }
    }

    // read keywords list
    private static List<String> readFile(String path, Integer... count) {
        List<String> keywords = new ArrayList<>();
        int num = 0;
        try {
            File myObj = new File(path);
            Scanner myReader = new Scanner(myObj);
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                keywords.add(data);
                num ++ ;
                if (count.length == 1 && num >= count[0]) {
                    break;
                }
            }
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return keywords;
    }


    public static void main(String[] args) {
        List<Document> doc = createDocuments(50000);
        int numThreads = 2;
        List<List> partitionedDocs = partitionData(doc, numThreads);
        // split documents
        long start = System.currentTimeMillis();
        List<List> results = new ArrayList<>();
        CountDownLatch latch = new CountDownLatch(numThreads);
        List<Thread> threads = new ArrayList<>();
        List<AwesomePipelineRunnable> executors = new ArrayList<>();
        for (List i : partitionedDocs) {
            AwesomePipelineRunnable executor = makeASingleParallelExecutor("split");
            executor.setMaterializedInput(i);
//            executor.setMaterializedResult(output);
            executor.setLatch(latch);
            executors.add(executor);
            threads.add(new Thread(executor));
        }
        for (Thread t : threads) {
            t.start();
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        for (AwesomePipelineRunnable exe : executors) {
            results.add((List) exe.getMaterializedOutput());
        }
        long end = System.currentTimeMillis();
        System.out.println("split total time: " + (end - start));
        start = System.currentTimeMillis();
        List<List> results2 = new ArrayList<>();
        CountDownLatch latch2 = new CountDownLatch(numThreads);
        List<String> stopWords = readFile("stopwords.txt");
        for (List i : results) {
            List output = new ArrayList<>();
            results2.add(output);
            AwesomePipelineRunnable executor = makeASingleParallelExecutor("filter", stopWords);
            executor.setMaterializedInput(i);
//            executor.setMaterializedResult(output);
            executor.setLatch(latch2);
            Thread p1 = new Thread(executor);
            p1.start();
//            System.out.println(p1.getName());
        }
        try {
            latch2.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        end = System.currentTimeMillis();
        System.out.println("filter totoal time: " + (end - start));



        // filter documents


    }

}

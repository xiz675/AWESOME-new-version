//package edu.sdsc.queryprocessing.scheduler.benchmark.unit;
//
//import edu.sdsc.datatype.execution.*;
//import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExecuteSQL;
//import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.FetchSQLResult;
//import edu.sdsc.queryprocessing.runnableexecutors.text.*;
//import edu.sdsc.utils.Pair;
//import org.jooq.*;
//import org.openjdk.jmh.annotations.*;
//import org.openjdk.jmh.annotations.Mode;
//import org.openjdk.jmh.annotations.Param;
//import org.openjdk.jmh.annotations.Scope;
//import org.openjdk.jmh.runner.Runner;
//import org.openjdk.jmh.runner.RunnerException;
//import org.openjdk.jmh.runner.options.Options;
//import org.openjdk.jmh.runner.options.OptionsBuilder;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
//@BenchmarkMode(Mode.SingleShotTime)
//@Warmup(iterations = 10, time = 300, timeUnit = TimeUnit.MILLISECONDS)
//@Measurement(iterations = 10, time = 300, timeUnit = TimeUnit.MILLISECONDS)
//@Fork(1)
//@State(Scope.Benchmark)
//@OutputTimeUnit(TimeUnit.MILLISECONDS)
//
//// this benchmark is to collect unit execution time of Case1Benchmark.
//public class Case1Benchmark {
////
//    @Param({"1000"})
//    private int size;
//    private Cursor<Record> x;
//    private List<AwesomeRecord> awesomeRecordResults = new ArrayList<>();
//    private List<Document> docs = new ArrayList<>();
//    private List<Document> splitDocs = new ArrayList<>();
//    private List<String> keywords;
//    private List<List<String>> keywordsDoc = new ArrayList<>();
//    private List<List<Pair<String, String>>> occurrence = new ArrayList<>();
//    private List<GraphElement> graphElement = new ArrayList<>();
//    private List<GraphElement> aggregatedGraph = new ArrayList<>();
//
//
//    @Setup
//    public void init() {
////        for (int i=0; i<8000; i++) {
////            Node n1 = new Node("word", "value", getRandomString(3));
////            Node n2 = new Node("word", "value", getRandomString(3));
////            Edge e = new Edge("cooccur");
////            e.setNodes(n1, n2, false);
////            aggregatedGraph.add(e);
////        }
//        // test split and keyword extraction
////        List<String> key = getKeyWordsFromFile("C:/Users/xw/Documents/lastName.txt", 300);
////        List<Pair<Integer, String>> rawDoc = create5000Docs(key, 5, 600, 0);
////        for (Pair<Integer, String> t : rawDoc) {
////            docs.add(new Document(t.first, t.second));
////        }
////        System.out.println(docs.size());
////        keywords = getKeyWordsFromFile("C:/Users/xw/Documents/lastName.txt", 500);
////        SplitDocumentsPhysical split = new SplitDocumentsPhysical(docs, splitDocs, new CountDownLatch(1), " ");
////        split.run();
////        System.out.println("split result: " + splitDocs.size());
//
//        String sql = "select id as newsid, news as newstext from toyset_600_300_5 limit " + size;
//        ExecuteSQL query = new ExecuteSQL(sql, "newsDB", "News");
//        query.run();
////        Result<Record> sqlResult = query.getResult();
//
//        // fetchResult, can not be executed in parallel
//        FetchSQLResult fetchRecord = new FetchSQLResult(query.result, awesomeRecordResults);
//        fetchRecord.run();
//
//        // create documents
//        CreateDocuments create = new CreateDocuments(awesomeRecordResults, docs,  "newsid", "newstext");
//        create.run();
//
//        // split documents
////        SplitDocumentsPhysical split = new SplitDocumentsPhysical(docs, splitDocs, new CountDownLatch(1), " ");
////        split.run();
////
////        // extract keywords from documents
////        keywords = getKeyWordsFromFile(getKeywordPath(), 500);
////        KeywordsExtraction getKeyWords = new KeywordsExtraction(splitDocs, keywordsDoc, new CountDownLatch(1), keywords);
////        getKeyWords.run();
////
////        // get co-occurrence of keywords
////        CollectCooccurance cooccur = new CollectCooccurance(keywordsDoc, occurrence, new CountDownLatch(1));
////        cooccur.run();
////
////        // collect graph element
////        CollectGraphElementFromListOfPairs collect = new CollectGraphElementFromListOfPairs(occurrence, graphElement, new CountDownLatch(1));
////        collect.run();
//
////        AggregateWeights aggregate = new AggregateWeights(graphElement);
////        aggregate.run();
////        List<GraphElement> aggregatedGraph = aggregate.getResult();
////        System.out.println(aggregatedGraph.size());
//    }
//
//
//    @Benchmark
//    public void createDocuments(){
//        List<Document> t = new ArrayList<>();
//        CreateDocuments create = new CreateDocuments(awesomeRecordResults, t,  "newsid", "newstext");
//        create.run();
//    }
////
////    @Benchmark
////    public void splitDocuments() {
//////        System.out.println("split documents size: " + docs.size());
////        List<Document> t = new ArrayList<>();
////        SplitDocumentsPhysical split = new SplitDocumentsPhysical(docs, t, new CountDownLatch(1), " ");
////        split.run();
////    }
////
////    @Benchmark
////    public void keywordsExtraction() {
//////        System.out.println("keywords extraction size: " + splitDocs.size());
////        List<List<String>> t = new ArrayList<>();
////        KeywordsExtraction getKeyWords = new KeywordsExtraction(splitDocs, t, new CountDownLatch(1), keywords);
////        getKeyWords.run();
////    }
//////
////    @Benchmark
////    public void collectCooccurance() {
//////        System.out.println("collect co-occurrence size: " + keywordsDoc.size());
////        List<List<Pair<String, String>>> t = new ArrayList<>();
////        CollectCooccurance cooccur = new CollectCooccurance(keywordsDoc, t, new CountDownLatch(1));
////        cooccur.run();
////    }
////
////    @Benchmark
////    public void collectGraphElement() {
//////        System.out.println("collect graph element size: " + occurrence.size());
////        List<GraphElement> t = new ArrayList<>();
//////        System.out.println("test: " + occurrence.size());
////        CollectGraphElementFromListOfPairs collect = new CollectGraphElementFromListOfPairs(occurrence, t, new CountDownLatch(1));
////        collect.run();
////    }
////
////
////    @Benchmark
////    public void aggregateWeights() {
//////        System.out.println("aggregate size: " + graphElement.size());
////        AggregateWeights aggregate = new AggregateWeights(graphElement);
////        aggregate.run();
//////        System.out.println(aggregate.getResult());
////    }
//
////    @Benchmark
////    public void test() {
////        try {
////            Thread.sleep(10);
////        } catch (InterruptedException e) {
////            e.printStackTrace();
////        }
////    }
//
//
////
////    @Benchmark
////    public void constructTinkerpopGraph() {
//////        System.out.println("construct graph size: " + aggregatedGraph.size());
////        // construct graph
////        ConstructTinkerpopGraph construct = new ConstructTinkerpopGraph(aggregatedGraph);
////        construct.run();
////    }
//
//
//
//    // benchmark executeSQL -> FetchSQL -> CreateDocumentsPhysical -> SplitByChars -> FilterStopWords-> CollectGraphElementFromRelation -> CreateGraph
//    public static void main(String[] args) throws RunnerException {
//        Options opt = new OptionsBuilder()
//                .include(Case1Benchmark.class.getSimpleName())
//                .forks(1)
//                .build();
//        new Runner(opt).run();
//    }
//
//
//
//}

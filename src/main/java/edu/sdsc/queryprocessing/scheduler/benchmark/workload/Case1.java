//package edu.sdsc.queryprocessing.scheduler.benchmark.workload;
//
//import edu.sdsc.datatype.execution.*;
//import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.ExecuteSQL;
//import edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution.FetchSQLResult;
//import edu.sdsc.queryprocessing.runnableexecutors.graph.AggregateWeights;
//import edu.sdsc.queryprocessing.runnableexecutors.graph.CollectGraphElementFromListOfPairs;
//import edu.sdsc.queryprocessing.runnableexecutors.text.CollectCooccurance;
//import edu.sdsc.queryprocessing.runnableexecutors.text.CreateDocuments;
//import edu.sdsc.queryprocessing.runnableexecutors.text.KeywordsExtraction;
//import edu.sdsc.queryprocessing.runnableexecutors.text.SplitDocuments;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.ExecutorMetaData;
//import edu.sdsc.queryprocessing.scheduler.utils.ExecutorEnum;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.CreateDocumentsMeta;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.KeywordsExtractionMeta;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.SplitDocumentsMeta;
//import edu.sdsc.utils.Pair;
//import org.apache.commons.cli.*;
//import org.jooq.Cursor;
//
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.IOException;
//import java.io.InputStream;
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.createdataset.Case1Dataset.readFile;
//import static edu.sdsc.queryprocessing.scheduler.utils.ParallelPipelineUtil.*;
//import static edu.sdsc.queryprocessing.executor.utils.ParallelUtil.partitionData;
//
//
//public class Case1 {
//    // fetchSQLResult -> createDocs -> splitDocs -> extract keywords -> get co occurrence
//    // -> collect graph elements -> aggregate weights
//    // have a map for operators and id
//    // for parallel execution, there is no need to set execution mode or capability for metadata
//    public static void parallelExecution(Integer docSize) throws InterruptedException {
//        int numThreads = 16;
//        long tempT;
//        // executeSQL
//        String sql = "select id as newsid, news as newstext from toyset_600_300_5 limit " + docSize;
//        tempT = System.currentTimeMillis();
//        ExecuteSQL query = new ExecuteSQL(sql, "newsDB", "News");
//        query.run();
//        System.out.println(String.format("executeSQL costs: %d ms", (System.currentTimeMillis() - tempT)));
////        Result<Record> sqlResult = query.getResult();
//
//        // fetchResult, can not be executed in parallel
//        long startTime = System.currentTimeMillis();
//        List<AwesomeRecord> awesomeRecordResults = new ArrayList<>();
//        tempT = System.currentTimeMillis();
//        FetchSQLResult fetchRecord = new FetchSQLResult(query.result, awesomeRecordResults);
//        fetchRecord.run();
//        System.out.println(String.format("fetchResult costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        ExecutorMetaData temp;
//        tempT = System.currentTimeMillis();
//        // partition SQL results for parallel execution
//        List<List<AwesomeRecord>> docsRecord = partitionData(awesomeRecordResults, numThreads);
//        // create documents execution, if do not set execution mode or capability, means it does not matter, it is block
//        temp = new CreateDocumentsMeta("newsid", "newstext");
//        List<List<Document>> docs = parallelExecutionUnit(docsRecord, numThreads, temp);
//        System.out.println(String.format("createDoc costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // split documents execution
//        tempT = System.currentTimeMillis();
//        temp = new SplitDocumentsMeta(" ");
//        List<List<Document>> splittedDocs = parallelExecutionUnit(docs, numThreads, temp);
//        System.out.println(String.format("splitDoc costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // extract keywords from documents
//        List<String> keywords = readFile(getKeywordPath(), 500);
//        tempT = System.currentTimeMillis();
//        temp = new KeywordsExtractionMeta(keywords);
//        List<List<List<String>>> keywordsFromDoc = parallelExecutionUnit(splittedDocs, numThreads, temp);
//        System.out.println(String.format("extractKeywords costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // get co-occurrence of keywords
//        tempT = System.currentTimeMillis();
//        temp = new ExecutorMetaData(ExecutorEnum.CollectCooccurance);
//        List<List<List<Pair<String, String>>>> cooccur = parallelExecutionUnit(keywordsFromDoc, numThreads, temp);
//        System.out.println(String.format("getCooccur costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // collect graph element
//        tempT = System.currentTimeMillis();
//        temp = new ExecutorMetaData(ExecutorEnum.CollectGraphElementFromListOfPairs);
//        List<List<GraphElement>> collectTemp = parallelExecutionUnit(cooccur, numThreads, temp);
//        System.out.println(String.format("collectGraphElement costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // aggregate graph element, it is a sequential operator, so need to merge lists of results to a single list as input
//        // can not be executed in parallel
//        tempT = System.currentTimeMillis();
//        List<GraphElement> collect = collectTemp.stream().flatMap(List::stream).collect(Collectors.toList());
//        AggregateWeights aggregate = new AggregateWeights(collect);
//        aggregate.run();
//        System.out.println(String.format("aggregateGraph costs: %d ms", (System.currentTimeMillis() - tempT)));
//        System.out.println(String.format("total cost for %d is %d ms", docSize, (System.currentTimeMillis() - startTime)));
//        List<GraphElement> aggregatedGraph = aggregate.getResult();
//        System.out.println(aggregatedGraph.size());
//
//    }
//
//    // the assignment is a list of single assignments, a single assignment is a list of pairs <Integer, Integer>
//    public static void greedyExecution(Integer docSize) throws InterruptedException {
//        // should create List<List<executorMetaData>>, and should call a function to get the List<List<coreNum>>
//        // then call the single assignment function for each batch
//        String sql = "select id as newsid, news as newstext from toyset_600_300_5 limit " + docSize;
//        ExecuteSQL query = new ExecuteSQL(sql, "newsDB", "News");
//        query.run();
//        // get input data for a single batch assignment
//        Cursor input = query.getResult();
//        // there are three SBA
//        // first one: fetch result -> create documents -> split documents
//        long startTime = System.currentTimeMillis();
//        List<Integer> assignment = new ArrayList<>();
//        assignment.add(1);
//        assignment.add(1);
//        assignment.add(14);
//        // create a list of executorMetaData, not final result, so should be pipeline
//        List<ExecutorMetaData> executors = new ArrayList<>();
//        ExecutorMetaData fetchSQL = new ExecutorMetaData(ExecutorEnum.FetchSQLResult, PipelineMode.streamoutput, PipelineMode.streamoutput);
//        ExecutorMetaData createDoc = new CreateDocumentsMeta("newsid", "newstext", PipelineMode.pipeline);
//        ExecutorMetaData splitDoc = new SplitDocumentsMeta(" ", PipelineMode.pipeline);
//        executors.add(fetchSQL);
//        executors.add(createDoc);
//        executors.add(splitDoc);
//        Object result1 = singleBatchAssign(executors, assignment, input, false);
//        executors = new ArrayList<>();
//        assignment = new ArrayList<>();
//        List<String> keywords = readFile(getKeywordPath(), 500);
//        ExecutorMetaData keywordsExtration = new KeywordsExtractionMeta(keywords, PipelineMode.pipeline);
//        keywordsExtration.setProducer(14);
//        executors.add(keywordsExtration);
//        assignment.add(16);
//        Object result2 = singleBatchAssign(executors, assignment, result1, false);
//        // System.out.println(((LinkedBlockingQueue) result2).size());
//
//        // third one: getCoocuur
//        executors = new ArrayList<>();
//        assignment = new ArrayList<>();
//        ExecutorMetaData cooccurMeta = new ExecutorMetaData(ExecutorEnum.CollectCooccurance, PipelineMode.pipeline, PipelineMode.pipeline);
//        cooccurMeta.setProducer(16);
//        executors.add(cooccurMeta);
//        assignment.add(16);
//        Object result3 = singleBatchAssign(executors, assignment, result2, false);
//        // System.out.println(((LinkedBlockingQueue) result3).size());
//
//        // fourth one: collectGraphElement -> aggregate weights
//        executors = new ArrayList<>();
//        assignment = new ArrayList<>();
//        ExecutorMetaData collectMeta = new ExecutorMetaData(ExecutorEnum.CollectGraphElementFromListOfPairs, PipelineMode.pipeline, PipelineMode.pipeline);
//        collectMeta.setProducer(16);
//        ExecutorMetaData aggregateMeta = new ExecutorMetaData(ExecutorEnum.AggregateWeights, PipelineMode.streaminput, PipelineMode.streaminput);
//        executors.add(collectMeta);
//        executors.add(aggregateMeta);
//        assignment.add(1);
//        assignment.add(1);
//        Object result4 = singleBatchAssign(executors, assignment, result3, true);
//        System.out.println(String.format("total cost for %d is %d ms", docSize, (System.currentTimeMillis() - startTime)));
//    }
//
//    public static void greedyExecutionLookAhead(Integer docSize) {
//        String sql = "select id as newsid, news as newstext from toyset_600_300_5 limit " + docSize;
//        ExecuteSQL query = new ExecuteSQL(sql, "newsDB", "News");
//        query.run();
//        // get input data for a single batch assignment
//        Cursor input = query.getResult();
//        // there are three SBA
//        // first one: fetch result -> create documents -> split documents
//        long startTime = System.currentTimeMillis();
//        List<Integer> assignment = new ArrayList<>();
//        assignment.add(1);
//        assignment.add(1);
//        assignment.add(14);
//        // create a list of executorMetaData, not final result, so should be pipeline
//        List<ExecutorMetaData> executors = new ArrayList<>();
//        ExecutorMetaData fetchSQL = new ExecutorMetaData(ExecutorEnum.FetchSQLResult, PipelineMode.streamoutput, PipelineMode.streamoutput);
//        ExecutorMetaData createDoc = new CreateDocumentsMeta("newsid", "newstext", PipelineMode.pipeline);
//        ExecutorMetaData splitDoc = new SplitDocumentsMeta(" ", PipelineMode.pipeline);
//        executors.add(fetchSQL);
//        executors.add(createDoc);
//        executors.add(splitDoc);
//        Object result1 = singleBatchAssign(executors, assignment, input, false);
//
//        // second one: keywords extraction, since it has multiple threads, should change it from block execution mode to stream input
//        // todo: should add parallel mode
//        executors = new ArrayList<>();
//        assignment = new ArrayList<>();
//        assignment.add(13);
//        assignment.add(1);
//        assignment.add(1);
//        assignment.add(1);
//        List<String> keywords = readFile(getKeywordPath(), 500);
//        ExecutorMetaData keywordsExtration = new KeywordsExtractionMeta(keywords, PipelineMode.pipeline);
//        keywordsExtration.setProducer(14);
//        ExecutorMetaData cooccurMeta = new ExecutorMetaData(ExecutorEnum.CollectCooccurance, PipelineMode.pipeline, PipelineMode.pipeline);
//        cooccurMeta.setProducer(7);
//        ExecutorMetaData collectMeta = new ExecutorMetaData(ExecutorEnum.CollectGraphElementFromListOfPairs, PipelineMode.pipeline, PipelineMode.pipeline);
//        ExecutorMetaData aggregateMeta = new ExecutorMetaData(ExecutorEnum.AggregateWeights, PipelineMode.streaminput, PipelineMode.streaminput);
//        executors.add(keywordsExtration);
//        executors.add(cooccurMeta);
//        executors.add(collectMeta);
//        executors.add(aggregateMeta);
//        Object result2 = singleBatchAssign(executors, assignment, result1, true);
//        System.out.println(String.format("total cost for %d is %d ms", docSize, (System.currentTimeMillis() - startTime)));
//    }
//
//
//
//
//    // sequentially execute, do not need latch for this
//    public static void sequentialExecution(Integer docSize) {
//        List<Document> docs = new ArrayList<>();
//        List<Document> splitDocs = new ArrayList<>();
//        List<List<String>> keywordsDoc = new ArrayList<>();
//        List<List<Pair<String, String>>> occurrence = new ArrayList<>();
//        List<GraphElement> graphElement = new ArrayList<>();
////        List<GraphElement> aggregatedGraph = new ArrayList<>();
//
////        long tempT;
//        // executeSQL
//        String sql = "select id as newsid, news as newstext from toyset_600_300_5 limit " + docSize;
////        tempT = System.currentTimeMillis();
//        ExecuteSQL query = new ExecuteSQL(sql, "newsDB", "News");
//        query.run();
////        System.out.println(String.format("executeSQL costs: %d ms", (System.currentTimeMillis() - tempT)));
////        Result<Record> sqlResult = query.getResult();
//
//        long startTime = System.currentTimeMillis();
//        // fetchResult, can not be executed in parallel
//        List<AwesomeRecord> awesomeRecordResults = new ArrayList<>();
////        tempT = System.currentTimeMillis();
//        FetchSQLResult fetchRecord = new FetchSQLResult(query.result, awesomeRecordResults);
//        fetchRecord.run();
////        System.out.println(String.format("fetchResult costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // create documents
//        System.out.println(awesomeRecordResults.size());
//        long tempT = System.currentTimeMillis();
////        createDocument(awesomeRecordResults, "newsid", "newstext");
//        CreateDocuments create = new CreateDocuments(awesomeRecordResults, docs,  "newsid", "newstext");
//        create.run();
//        System.out.println(String.format("createDoc costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // split documents
//        tempT = System.currentTimeMillis();
//        SplitDocuments split = new SplitDocuments(docs, splitDocs, " ");
//        split.run();
//        System.out.println(String.format("splitDoc costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // extract keywords from documents
//        List<String> keywords = readFile(getKeywordPath(), 500);
//        tempT = System.currentTimeMillis();
//        KeywordsExtraction getKeyWords = new KeywordsExtraction(splitDocs, keywordsDoc, keywords);
//        getKeyWords.run();
//        System.out.println(String.format("extractKeywords costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // get co-occurrence of keywords
//        tempT = System.currentTimeMillis();
//        CollectCooccurance cooccur = new CollectCooccurance(keywordsDoc, occurrence);
//        cooccur.run();
//        System.out.println(String.format("getCooccur costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // collect graph element
//        tempT = System.currentTimeMillis();
//        CollectGraphElementFromListOfPairs collect = new CollectGraphElementFromListOfPairs(occurrence, graphElement);
//        collect.run();
//        System.out.println(String.format("collectGraphElement costs: %d ms", (System.currentTimeMillis() - tempT)));
//
//        // aggregate graph element, it is a sequential operator, so need to merge lists of results to a single list as input
//        // can not be executed in parallel
//        tempT = System.currentTimeMillis();
//        AggregateWeights aggregate = new AggregateWeights(graphElement);
//        aggregate.run();
//        List<GraphElement> aggregatedGraph = aggregate.getResult();
//        System.out.println(String.format("aggregateGraph costs: %d ms", (System.currentTimeMillis() - tempT)));
//        System.out.println(String.format("total cost for %d is %d ms", docSize, (System.currentTimeMillis() - startTime)));
//        System.out.println(aggregatedGraph.size());
//    }
//
//
//    public static String getKeywordPath() {
//        Properties prop = new Properties();
//        InputStream input = null;
//        String path = null;
//
//        try {
//            input = new FileInputStream("config.properties");
//            try {
//                prop.load(input);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//
//            path = prop.getProperty("keyword_path");
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        return path;
//    }
//
//
//
//    public static void main(String[] args) throws InterruptedException {
//        // add command line parameters
//        Options options = new Options();
//
//        Option mode = new Option("m", "mode", true, "execution mode");
//        // input.setRequired(true);
//
//        Option size = new Option("s", "size", true, "document size");
//
//        options.addOption(mode);
//        options.addOption(size);
//
//        CommandLineParser parser = new DefaultParser();
//        final CommandLine commandLine;
//        String m = null;
//        String s = null;
//        try {
//            commandLine = parser.parse(options, args);
//            m = commandLine.getOptionValue("m");
//            s = commandLine.getOptionValue("s");
//        } catch (org.apache.commons.cli.ParseException e) {
//            System.out.println("exceptions reading parameters " + e.getMessage());
//        }
//        int docSize = Integer.parseInt(s);
//        List<List<Integer>> assignment = new ArrayList<>();
//        assert m != null;
//        switch (m) {
//            case "greedy": {
//                greedyExecution(docSize);
//                break;
//            }
//            case "greedy-look-ahead": {
//                greedyExecutionLookAhead(docSize);
//                break;
//            }
//            case "parallel":
//                parallelExecution(docSize);
//                break;
//            default:
//                assert m.equals("sequential");
//                sequentialExecution(docSize);
//                break;
//        }
//    }
//}
//
//        // construct graph
////        ConstructTinkerpopGraph construct = new ConstructTinkerpopGraph(aggregatedGraph);
////        construct.run();
////        Graph g = construct.getResult();

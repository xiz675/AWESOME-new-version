//package edu.sdsc.queryprocessing.scheduler.utils;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.Document;
//import edu.sdsc.datatype.execution.GraphElement;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.DBMSExecution.FetchSQLResult;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.baserunnable.AwesomeBlockRunnable;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.baserunnable.AwesomeRunnable;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.baserunnable.AwesomeStreamInputRunnable;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.graph.*;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.text.*;
//import edu.sdsc.queryprocessing.scheduler.utils.metadata.*;
//import edu.sdsc.utils.Pair;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.jooq.Cursor;
//import org.jooq.Record;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class ParallelPipelineUtil {
//    // todo: this is only for a single chain, and for a DAG, each single assignment have operators from different chains, and should maintain a map with variable id to result
//    //    if an operator has a blocking or streaminput execution mode, then should store the result to the Map
//    // for a single batch assignment, for the first operator, it needs to have materialized input assigned.
//    // first operator may not be List and last operator may not generate list. These two operators need to be handled in a different way
//    // todo: should add capability and execution mode to ExecutorMetaData, if the last operator has capability stream input, then should handle it differently
//    // if not final result, should still output a LinkedList
//    public static Object singleBatchAssign(List<ExecutorMetaData> operators, List<Integer> cores, Object input, boolean finalResult) {
//        assert operators.size() == cores.size();
//        assert operators.size() > 0;
//        Object result;
//        if (finalResult) {
//            result = Collections.synchronizedList(new ArrayList<>());
//        }
//        else {
//            result = new LinkedBlockingQueue<>();
//        }
//        List<LinkedBlockingQueue> tempRes = new ArrayList<>();
////        CountDownLatch latch = new CountDownLatch(cores.size());
//        CountDownLatch latch = new CountDownLatch(cores.stream().reduce(0, Integer::sum));
//        // if there is only one operator, then follow the last operator's rule
//        for (int i=0; i<operators.size()-1; i++) {
//            AwesomeRunnable t;
//            if (i==0) {
//                LinkedBlockingQueue temp = new LinkedBlockingQueue();
//                tempRes.add(temp);
//                for (int c=0; c<cores.get(i); c++) {
//                    t = makeExecutor(operators.get(i), input, temp, operators.get(i).getExecutionMode());
//                    t.setLatch(latch);
//                    Thread p = new Thread(t);
//                    p.start();
//                }
//            }
//            // todo: for now only consider the last operator, but should change it to has a stream input or blocking execution mode
//            else {
//                LinkedBlockingQueue temp = new LinkedBlockingQueue();
//                tempRes.add(temp);
//                for (int c=0; c<cores.get(i); c++) {
//                    t = makeExecutor(operators.get(i), tempRes.get(i-1), temp, PipelineMode.pipeline);
//                    t.setLatch(latch);
//                    Thread p = new Thread(t);
//                    p.start();
//                }
//            }
//        }
//        int lastIndex = operators.size()-1;
//        ExecutorMetaData meta = operators.get(lastIndex);
//        // if the last operator returns a whole result, then there must be only one thread
//        if (meta.getPipelineCap().equals(PipelineMode.block) || meta.getPipelineCap().equals(PipelineMode.streaminput)) {
//            AwesomeRunnable lastOpe = makeLastOperatorWithBlockingResult(meta, tempRes.get(lastIndex - 1), meta.getExecutionMode());
//            lastOpe.setLatch(latch);
//            Thread p = new Thread(lastOpe);
//            p.start();
//            try {
//                latch.await();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            // get execution mode
//            if (meta.getPipelineCap().equals(PipelineMode.streaminput)) {
//                return ((AwesomeStreamInputRunnable) lastOpe).getMaterializedOutput();}
//            else {
//                return ((AwesomeBlockRunnable) lastOpe).getMaterializedOutput();}
//        }
//        // else there can be more threads and the result is a list.
//        else {
//            Integer lastCoreNum = cores.get(lastIndex);
//            for (int c=0; c<lastCoreNum; c++) {
//                AwesomeRunnable lastOpe;
//                if (lastIndex == 0) {
//                    lastOpe = makeExecutor(meta, input, result, meta.getExecutionMode());
//                }
//                else {
//                    lastOpe = makeExecutor(meta, tempRes.get(lastIndex-1), result, meta.getExecutionMode());
//                }
//                lastOpe.setLatch(latch);
//                Thread p = new Thread(lastOpe);
//                p.start();
//            }
//            try {
//                latch.await();
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            return result;
//        }
//    }
//
////
////
//////    public static<I, O> List<O> singleBatchAssignInMiddle(List<ExecutorMetaData> operators, List<Integer> cores, List<I> inputData) {
//////        // if the input  needs to be partitioned
//////
//////        // if the input does not need to be partitioned
//////
//////        // for the other operators,
//////
//////
//////
//////    }
////
//
////
//    // todo: needs to add other operators
//    // this is only for operators which has stream output or pipeline capbility
//    // the input will be the one which the operator has capbility on, other input will be assigned as metadata
//    private static AwesomeRunnable makeExecutor(ExecutorMetaData executor, Object input, Object output, PipelineMode mode) {
////        assert mode.equals(PipelineMode.block) || mode.equals(PipelineMode.streamoutput);
//        ExecutorEnum name = executor.getExecutor();
//        AwesomeRunnable temp;
//        switch (name) {
//            case GetNeighbor: {
//                temp = new GetNeighbors((Graph) input);
//                break;
//            }
//            case Coreness: {
//                if (mode.equals(PipelineMode.streamoutput)) {
//                    CorenessMeta core = (CorenessMeta) executor;
//                    temp = new Coreness((Graph) input, core.getNeighbors(), (LinkedBlockingQueue<List<Pair<Integer, Long>>>) output);
//                }
//                else {
//                    assert mode.equals(PipelineMode.block);
//                    temp = new Coreness((Graph) input, ((CorenessMeta) executor).getNeighbors());
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            case Corerank: {
//                // todo: this has four constructors, add others
//                assert mode.equals(PipelineMode.pipeline);
//                temp = new CoreRank(((CoreRankMeta) executor).getNeighbors(),
//                        (LinkedBlockingQueue<List<Pair<Integer, Long>>>) input, (LinkedBlockingQueue<List<Pair<Integer, Integer>>>) output);
//                break;
//            }
//            case CollectCooccurance: {
//                if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new CollectCooccurance((List<List<String>>) input, (LinkedBlockingQueue<List<List<Pair<String, String>>>>) output);
//                }
//                else if (mode.equals(PipelineMode.pipeline)) {
//                    temp = new CollectCooccurance((LinkedBlockingQueue<List<List<String>>>) input, (LinkedBlockingQueue<List<List<Pair<String, String>>>>) output, executor.getProducer());
//                }
//                else if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new CollectCooccurance((LinkedBlockingQueue<List<List<String>>>) input, (LinkedBlockingQueue<List<List<Pair<String, String>>>>) output, executor.getProducer());
//                }
//                else {
//                    temp = new CollectCooccurance((List<List<String>>) input);
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            case CreateDocuments: {
//                CreateDocumentsMeta meta = (CreateDocumentsMeta) executor;
//                if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new CreateDocuments((LinkedBlockingQueue<List<AwesomeRecord>>) input, meta.getDocID(), meta.getTextID(), executor.getProducer());
//                    temp.setMaterializedOutput(output);
//                }
//                else if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new CreateDocuments((List<AwesomeRecord>) input, (LinkedBlockingQueue<List<Document>>) output, meta.getDocID(), meta.getTextID());
//                }
//                else if (mode.equals(PipelineMode.pipeline)) {
//                    temp = new CreateDocuments((LinkedBlockingQueue<List<AwesomeRecord>>) input, (LinkedBlockingQueue<List<Document>>) output, meta.getDocID(), meta.getTextID(), executor.getProducer());
//                }
//                else {
//                    temp = new CreateDocuments((List<AwesomeRecord>) input, meta.getDocID(), meta.getTextID());
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            case KeywordsExtraction: {
//                KeywordsExtractionMeta meta = (KeywordsExtractionMeta) executor;
//                if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new KeywordsExtraction((LinkedBlockingQueue<List<Document>>) input, meta.getKeywords(), meta.getProducer());
//                    temp.setMaterializedOutput(output);
//                }
//                else if (mode.equals(PipelineMode.pipeline)) {
//                    temp = new KeywordsExtraction((LinkedBlockingQueue<List<Document>>) input, (LinkedBlockingQueue<List<List<String>>>) output, meta.getKeywords(), meta.getProducer());
//                }
//                else if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new KeywordsExtraction((List<Document>) input, (LinkedBlockingQueue<List<List<String>>>) output, meta.getKeywords());
//                }
//                else {
//                    // block
//                    temp = new KeywordsExtraction((List<Document>) input,  meta.getKeywords());
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            case CollectGraphElementFromListOfPairs: {
//                if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new CollectGraphElementFromListOfPairs((List<List<Pair<String, String>>>) input, (LinkedBlockingQueue<List<GraphElement>>) output);
//                }
//                else if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new CollectGraphElementFromListOfPairs((LinkedBlockingQueue<List<List<Pair<String, String>>>>) input, executor.getProducer());
//                    temp.setMaterializedOutput(output);
//                }
//                else if (mode.equals(PipelineMode.pipeline)) {
//                    temp = new CollectGraphElementFromListOfPairs((LinkedBlockingQueue<List<List<Pair<String, String>>>>) input, (LinkedBlockingQueue<List<GraphElement>>) output, executor.getProducer());
//                }
//                else {
//                    temp = new CollectGraphElementFromListOfPairs((List<List<Pair<String, String>>>) input);
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            case SplitDocuments: {
//                SplitDocumentsMeta meta = (SplitDocumentsMeta) executor;
//                if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new SplitDocuments((LinkedBlockingQueue<List<Document>>) input, (List<Document>) output, meta.getSplitter(), meta.getProducer());
//                }
//                else if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new SplitDocuments((List<Document>) input, (LinkedBlockingQueue<List<Document>>)  output, meta.getSplitter());
//                }
//                else if (mode.equals(PipelineMode.pipeline)) {
//                    temp = new SplitDocuments((LinkedBlockingQueue<List<Document>>) input, (LinkedBlockingQueue<List<Document>>) output, meta.getSplitter(), meta.getProducer());
//                }
//                else {
//                    temp = new SplitDocuments((List<Document>) input, (List<Document>) output, meta.getSplitter());
//                }
//                break;
//            }
//            case FilterDocuments: {
//                FilterDocumentsMeta meta = (FilterDocumentsMeta) executor;
//                if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new FilterDocuments((LinkedBlockingQueue<List<Document>>) input, meta.getStopwords(), meta.getProducer());
//                    temp.setMaterializedOutput(output);
//                }
//                else if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new FilterDocuments((List<Document>) input, (LinkedBlockingQueue<List<Document>>) output, meta.getStopwords());
//                }
//                else if (mode.equals(PipelineMode.pipeline)) {
//                    temp = new FilterDocuments((LinkedBlockingQueue<List<Document>>) input, (LinkedBlockingQueue<List<Document>>) output, meta.getStopwords(), meta.getProducer());
//                }
//                else {
//                    temp = new FilterDocuments((List<Document>) input, meta.getStopwords());
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            case FetchSQLResult: {
//                if (mode.equals(PipelineMode.streamoutput)) {
//                    temp = new FetchSQLResult((Cursor< Record >) input, (LinkedBlockingQueue<List<AwesomeRecord>>) output);
//                }
//                else {
//                    temp = new FetchSQLResult((Cursor<Record>) input);
//                    temp.setMaterializedOutput(output);
//                }
//                break;
//            }
//            default: {
//                temp = new CollectCooccurance((List<List<String>>) input, (LinkedBlockingQueue<List<List<Pair<String, String>>>>) output);
//                break;
//            }
//        }
//        return temp;
//    }
//
//
//    // this is for operator which can't generate output as a stream (streaminput or block)
//    // todo: add more operators
//    public static AwesomeRunnable makeLastOperatorWithBlockingResult(ExecutorMetaData executor, Object input, PipelineMode mode) {
//        ExecutorEnum name = executor.getExecutor();
//        AwesomeRunnable temp;
//        switch (name) {
//            case AggregateWeights: {
//                if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new AggregateWeights((LinkedBlockingQueue<List<GraphElement>>) input);
//                }
//                else {
//                    temp = new AggregateWeights((List<GraphElement>) input);
//                }
//                break;
//            }
//            case ConstructTinkerpopGraph: {
//                if (mode.equals(PipelineMode.streaminput)) {
//                    temp = new ConstructTinkerpopGraph((LinkedBlockingQueue<List<GraphElement>>) input);
//                }
//                else {
//                    // block
//                    temp = new ConstructTinkerpopGraph((List<GraphElement>) input);
//                }
//                break;
//            }
////            case ExecuteSQL: {
////                assert mode.equals(PipelineMode.block);
////                ExecuteSQLMeta meta = (ExecuteSQLMeta) executor;
////                temp = new ExecuteSQL(meta.getSql(), meta.getPolystore(), meta.getDbName());
////                break;
////            }
//            case AddNodeProperty: {
//                // todo: add another constructor, add other value types
//                assert mode.equals(PipelineMode.streaminput);
//                NodePropertyMeta meta = (NodePropertyMeta) executor;
//                String valueType = meta.getValueType();
//                assert valueType.equals("Integer");
//                temp = new AddNodeProperty<Integer>(meta.getPropertyName(), meta.getGraph(), (LinkedBlockingQueue<List<Pair<Integer, Integer>>>) input);
//                break;
//            }
//            default:
//                temp = new AggregateWeights((List<GraphElement>) input);
//        }
//        return temp;
//    }
//
//
//
//    // rewrite this to the form of using
//    public static void main(String[] args) throws InterruptedException {
//
////
//////        Result<Record> sqlResult = query.getResult();
////        CountDownLatch latch = new CountDownLatch(8);
////        // fetchResult, can not be executed in parallel
////        LinkedBlockingQueue awesomeRecordResults = new LinkedBlockingQueue<>();
////        LinkedBlockingQueue docs = new LinkedBlockingQueue<>();
////        List splitDocs = Collections.synchronizedList(new ArrayList<>());
////        FetchSQLResult fetchRecord = new FetchSQLResult(query.result, awesomeRecordResults);
////        fetchRecord.setLatch(latch);
////        Thread p1 = new Thread(fetchRecord);
////        p1.start();
////        CreateDocumentsPhysical create = new CreateDocumentsPhysical(awesomeRecordResults, docs,  "newsid", "newstext");
////        create.setLatch(latch);
////        Thread p2 = new Thread(create);
////        p2.start();
////        //        SplitDocumentsPhysical split = new SplitDocumentsPhysical(docs, splitDocs, latch, " ");
//////        Thread p3 = new Thread(split);
//////        p3.start();
//////
////        for (int i=0; i<6; i++) {
////            SplitDocumentsPhysical split = new SplitDocumentsPhysical(docs, splitDocs, " ");
////            split.setLatch(latch);
////            Thread p3 = new Thread(split);
////            p3.start();
////        }
////        latch.await();
////        System.out.println(splitDocs.size());
//
//
//
//        // test pipeline framework
//        // for pipeline, no matter how many producer threads, it add to the same blocking queue, only one blocking queue, and the consumer
//        // consume from one blocking queue.
//        // if the consumer needs to get data from multiple producers, then should only stop it when all of the end has been reached
////        List<Integer> data = makeData(100);
////        LinkedBlockingQueue<List<Integer>> res1 = new LinkedBlockingQueue<>();
////        List<Integer> res2 = new ArrayList<>();
////        CountDownLatch latch = new CountDownLatch(5);
//////        AddOne t = new AddOne(data, res1, latch);
//////        Thread p = new Thread(t);
//////        p.start();
////        List<List<Integer>> d = partitionData(data, 5);
//////        CountDownLatch latch = new CountDownLatch(6);
////        for (List<Integer> x : d) {
////            AddOne t = new AddOne(x, res1, latch);
////            Thread p = new Thread(t);
////            p.start();
////        }
////        latch.await();
//////        System.out.println(res1.size());
////
////        latch = new CountDownLatch(1);
////        AddTwo tt = new AddTwo(res1, res2, latch, 5);
////        Thread pp = new Thread(tt);
////        pp.start();
////        latch.await();
////        System.out.println(res2.size());
//
//    }
//
//}

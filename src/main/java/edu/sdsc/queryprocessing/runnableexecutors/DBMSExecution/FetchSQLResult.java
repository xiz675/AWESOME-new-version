//package edu.sdsc.queryprocessing.depreciatedrunnableexecutors.DBMSExecution;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.depreciatedrunnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
//import org.jooq.*;
//import org.jooq.Record;
//
//import java.util.*;
//import java.util.concurrent.LinkedBlockingQueue;
//
//
//public class FetchSQLResult extends AwesomeStreamOutputRunnable<Cursor<Record>, AwesomeRecord> {
//    final private Integer unitSize = 500;
//
//    // the latch is for synchronize with the following operators in the pipeline
//    public FetchSQLResult(Cursor<Record> data, LinkedBlockingQueue<List<AwesomeRecord>> result) {
//        super(data, result);
//        setExecutionMode(PipelineMode.streamoutput);
//    }
//
//    public FetchSQLResult(Cursor<Record> data) {
//        super(data);
//        setExecutionMode(PipelineMode.block);
//    }
////    public List<AwesomeRecord> getAwesomeRecords() {
////
////
////    }
//
//    // each list has unit size 500
//    @Override
//    public void executeStreamOutput() {
//        LinkedBlockingQueue<List<AwesomeRecord>> streamResult = getStreamResult();
//        Cursor<Record> input = getMaterializedInput();
//        List<AwesomeRecord> unit;
////        int count = unitSize;
//        try {
//            System.out.println("executeSQL starts at: " + System.currentTimeMillis() + " ms");
//            while (input.hasNext()) {
////                start = System.currentTimeMillis();
//                Result<Record> t = input.fetchNext(unitSize);
//                unit = new ArrayList<>();
//                for (Record r : t) {
//                    unit.add(new AwesomeRecord(r));
//                }
//                streamResult.put(unit);
//                // System.out.println("fetch " + unit.size() + " sql result " + times + ": " + (System.currentTimeMillis() - start) + " ms");
//                    //count = unitSize;
////                times += 1;
//            }
//            streamResult.put(Collections.emptyList());
//            System.out.println("fetch sql count down: " + getLatch().getCount() + " at " + System.currentTimeMillis() );
//            countDown();
//        }
//        catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Override
//    public void executeBlocking() {
//        Cursor<Record> input = getMaterializedInput();
//        List<AwesomeRecord> materializedOutput = new ArrayList<>();
//        while (input.hasNext()) {
//            Result<Record> t = input.fetchNext(unitSize);
//            for (Record r : t) {
//                materializedOutput.add(new AwesomeRecord(r));
//            }
////            System.out.println("fetch " + unitSize + " cost:" + (System.currentTimeMillis() - start));
//        }
//        setMaterializedOutput(materializedOutput);
////        for (Record i : input) {
////        }
////        materializedOutput.add(new AwesomeRecord(i));
////        materializedOutput = input.map(i -> new AwesomeRecord(i));
//    }
//
//
//    public static void main(String[] args) {
//        Map<Integer, String> s = new HashMap<>();
//        s.put(1, "s");
//        Map<Integer, Map<Integer, String>> m = new HashMap<>();
//        m.put(1, s);
//        Map<Integer, String> a = m.get(1);
//        a.put(2, "a");
//        System.out.println(m.get(1).keySet());
//    }
//}

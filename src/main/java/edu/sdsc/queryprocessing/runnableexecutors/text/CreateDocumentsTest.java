//package edu.sdsc.queryprocessing.executors.text;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.Document;
//import edu.sdsc.datatype.execution.Mode;
//import edu.sdsc.queryprocessing.executors.baserunnable.AwesomeRunnable;
//import edu.sdsc.queryprocessing.executors.baserunnable.AwesomeStreamOutputRunnable;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.LinkedBlockingQueue;
//
//public class CreateDocumentsTest extends AwesomeStreamOutputRunnable<Integer, Document> {
//
//    public CreateDocumentsTest(Mode m) {
//        super(m);
//    }
//
//    // stream out constructor
//    public CreateDocumentsTest(Integer data, LinkedBlockingQueue<List<Document>> result, CountDownLatch latch) {
//        super(data, result, latch);
//    }
//
//    // blocking constructor:
//    public CreateDocumentsTest(Integer input, List<Document> result) {
//        super(input, result);
//    }
//
//
////    // for queue output, the queue is passed to constructor
////    public List<Integer> getMaterializedResult() {
////        Mode executionMode = getExecutionMode();
////        assert (executionMode.equals(Mode.block) || executionMode.equals(Mode.streaminput));
////        return getMaterializedResult();
////    }
//
//    // result has an EOF
//    public void executeStreamOutput() {
//        Integer size = getMaterializedInput();
//        LinkedBlockingQueue<Document> streamResult = getStreamResult();
//        StringBuilder x = new StringBuilder();
//        for (int i=0; i<500; i++) {
//            x.append(" xiuwen");
//        }
//        try {
//            for (int i=0; i<size; i++) {
//                Document temp = new Document(i, x.toString());
//                streamResult.add(temp);
//            }
//            streamResult.put(new Document());
//            countDown();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
//
//    public void executeBlocking() {
//        int size = getMaterializedInput();
//        List<Document> materializedResult = getMaterializedResult();
//        StringBuilder x = new StringBuilder();
//        for (int i=0; i<500; i++) {
//            x.append(" xiuwen");
//        }
//        for (int i=0; i<size; i++) {
//            Document temp = new Document(i, x.toString());
//            materializedResult.add(temp);
//        }
////        countDown();
//    }
//}

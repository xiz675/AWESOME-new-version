//package edu.sdsc.queryprocessing.executor.execution.executionunit.textoperations;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.PipelineExeutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.SplitDocumentsPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//
//import java.util.Arrays;
//
//import static edu.sdsc.queryprocessing.executor.utils.MaterializeStream.materializeStreamEntry;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class ExeSplitDocuments extends PipelineExeutionBase {
//    private String splitter;
//    private MaterializedDocuments docs;
//    private StreamDocuments streamDocs;
//    private StreamDocuments streamResult;
//
//    public ExeSplitDocuments(SplitDocumentsPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        this.splitter = ope.getSplitter();
//        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        if (mode.equals(PipelineMode.streamoutput) || mode.equals(PipelineMode.block)) {
//            this.docs = (MaterializedDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else if (mode.equals(PipelineMode.materializestream)) {
//            this.streamResult = (StreamDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else {
//            this.streamDocs = (StreamDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//    }
//
//
//    @Override
//    public MaterializedDocuments execute() {
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
////        List<Document> docs = ((MaterializedDocuments) evt.getTableEntry(inputVar)).getValue();
////        Publisher<Document> tokenized = executeStreamInputStreamOutput(Flowable.fromIterable(docs));
//        this.streamResult = executeStreamOutput();
//        return materialize();
////        MaterializedDocuments(Flowable.fromPublisher(tokenized).toList().blockingGet());
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, result);
//    }
//
//    @Override
//    public MaterializedDocuments executeStreamInput() {
//        this.streamResult = executeStreamInputStreamOutput();
//        return materialize();
////        List<Document> rsult = Flowable.fromPublisher(tokenized).toList().blockingGet();
////        MaterializedDocuments docs = new MaterializedDocuments(rsult);
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, docs);
//    }
//
//    @Override
//    public StreamDocuments executeStreamOutput() {
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
////        List<Document> docs = ((MaterializedDocuments) evt.getTableEntry(inputVar)).getValue();
//        this.streamDocs = new StreamDocuments(Flowable.fromIterable(docs.getValue()));
//        return executeStreamInputStreamOutput();
//    }
//
//    @Override
//    public StreamDocuments executeStreamInputStreamOutput() {
//        return new StreamDocuments(Flowable.fromPublisher(streamDocs.getValue()).observeOn(Schedulers.computation()).map(s -> {
//            s.setTokens(Arrays.asList(s.text.split(splitter)));
//            return s;}));
//
////        if (!input.isParallel()) {input = input.parallel();}
////        return input.map(s -> {
////            s.setTokens(Arrays.asList(s.text.split(splitter)));
////            return s;
////        });
//    }
//
//    @Override
//    public MaterializedDocuments materialize() {
//        return (MaterializedDocuments) materializeStreamEntry(this.streamResult);
//    }
//}

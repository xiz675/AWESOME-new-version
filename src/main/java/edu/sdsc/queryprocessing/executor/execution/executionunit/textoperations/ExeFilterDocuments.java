//package edu.sdsc.queryprocessing.executor.execution.executionunit.textoperations;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.PipelineExeutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.FilterDocumentsPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//
//import java.io.BufferedReader;
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.IOException;
//import java.util.ArrayList;
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.executor.utils.MaterializeStream.materializeStreamEntry;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class ExeFilterDocuments extends PipelineExeutionBase {
//    private List<String> stopwords;
//    private MaterializedDocuments docs;
//    private StreamDocuments streamDocs;
//    private StreamDocuments streamResult;
//
//    public ExeFilterDocuments(FilterDocumentsPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        // set mode and assign the proper value
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        this.stopwords = getStopWords(ope.getStopwords());
//        if (mode.equals(PipelineMode.materializestream)) {
//            this.streamResult = (StreamDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else {
//            if (mode.equals(PipelineMode.streamoutput) || mode.equals(PipelineMode.block)) {
//                this.docs = (MaterializedDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//            }
//            else {
//                this.streamDocs = (StreamDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//            }
//        }
//    }
//
//
//    @Override
//    public MaterializedDocuments execute() {
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
////        List<Document> docs = ((MaterializedDocuments) evt.getTableEntry(inputVar)).getValue();
//        this.streamDocs = new StreamDocuments(Flowable.fromIterable(this.docs.getValue()));
//        return executeStreamInput();
////        MaterializedDocuments result = new MaterializedDocuments(Flowable.fromPublisher(filtered).toList().blockingGet());
////        return result;
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, result);
//    }
//
//
//    @Override
//    public MaterializedDocuments executeStreamInput() {
//        this.streamResult = executeStreamInputStreamOutput();
//        return materialize();
////        Publisher<Document> filtered = executeStreamInputStreamOutput(input);
////        List<Document> rsult = Flowable.fromPublisher(filtered).toList().blockingGet();
//////        MaterializedDocuments docs = new MaterializedDocuments(rsult);
////        return new MaterializedDocuments(rsult);
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, docs);
//    }
//
//    @Override
//    public StreamDocuments executeStreamOutput() {
//        this.streamDocs = new StreamDocuments(Flowable.fromIterable(this.docs.getValue()));
//        return executeStreamInputStreamOutput();
//    }
//
//    @Override
//    public StreamDocuments executeStreamInputStreamOutput() {
////        if (!input.isParallel()) {input = input.parallel();}
//
//        return new StreamDocuments(Flowable.fromPublisher(this.streamDocs.getValue()).observeOn(Schedulers.computation()).map(
//                s ->
//                {s.tokens = Flowable.fromIterable(s.tokens).filter(x -> !stopwords.contains(x)).toList().blockingGet();
//                return s;}
//                ));
////        return input.map(s -> {s.tokens = s.tokens.stream().filter(x -> !stopwords.contains(x)).collect(Collectors.toList()); return s;});
//    }
//
//    @Override
//    public MaterializedDocuments materialize() {
//        return (MaterializedDocuments) materializeStreamEntry(this.streamResult);
//    }
//
//
//    private List<String> getStopWords(String path) {
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
//}

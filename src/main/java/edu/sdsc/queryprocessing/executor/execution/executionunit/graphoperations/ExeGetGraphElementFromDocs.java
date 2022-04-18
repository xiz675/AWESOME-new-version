//package edu.sdsc.queryprocessing.executor.execution.executionunit.graphoperations;
//
//import edu.sdsc.datatype.execution.*;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.PipelineExeutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromDocsPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//import org.reactivestreams.Publisher;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static edu.sdsc.queryprocessing.executor.utils.MaterializeStream.materializeStreamEntry;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//
//// mark: the materialize method take a different stream
//public class ExeGetGraphElementFromDocs extends PipelineExeutionBase {
//    private Integer distance;
//    private List<String> words;
////    private BuildGraphFromDocsPhysical ope;
////    private ExecutionVariableTable evt;
//    private MaterializedDocuments materializedDocs;
//    private StreamDocuments streamDocs;
//    private StreamGraphData inputStream;
//
//    public ExeGetGraphElementFromDocs(BuildGraphFromDocsPhysical ope, ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//        // set parameters from operator's parameters
//        // todo: this should be moved to physical part
////        List<FunctionParameter> parameters = translateParameters(ope.getParameters());
//        FunctionParameter temp = ope.getMaxDistance();
//        if (temp != null) {
//            this.distance = (Integer) temp.getValueWithExecutionResult(evt, localEvt);
//        }
//        temp = ope.getWords();
//        if (temp != null) {
//            this.words = (List<String>) temp.getValueWithExecutionResult(evt, localEvt);
//        }
//        // set mode and assign the proper value
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        // since function can have more than one variable parameter, should get the one needed with key
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        if (mode.equals(PipelineMode.streamoutput) || mode.equals(PipelineMode.block)) {
//            // get the variable id and then get table entry
//            Pair<Integer, String> inputVar = ope.getDocID();
//            this.materializedDocs = (MaterializedDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else {
//            Pair<Integer, String> inputVar = ope.getDocID();
//            this.streamDocs = (StreamDocuments) getTableEntryWithLocal(inputVar, evt, localEvt);
////            this.streamDocs = (StreamDocuments) parameters.get(0).getValueWithExecutionResult(evt);
//        }
//    }
//
//
//    @Override
//    public MaterializedGraphData execute() {
//        this.inputStream = executeStreamOutput();
//        return materialize();
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
////        List<Document> docs = ((MaterializedDocuments) evt.getTableEntry(inputVar)).getValue();
////        List<GraphElement> rawdata = Flowable.fromPublisher(executeStreamInputStreamOutput(Flowable.fromIterable(docs))).toList().blockingGet();
////        return new MaterializedGraphData(rawdata);
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, new ExecutionGraphElement(rawdata));
//    }
//
//
//    @Override
//    public MaterializedGraphData executeStreamInput() {
//        return (MaterializedGraphData) materializeStreamEntry(executeStreamInputStreamOutput());
////        List<GraphElement> rawdata = Flowable.fromPublisher().toList().blockingGet();
//////                collect(Collectors.toList());
////        MaterializedGraphData graph = new MaterializedGraphData(rawdata);
////        return graph;
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, graph);
//    }
//
//    @Override
//    public StreamGraphData executeStreamOutput() {
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
////        List<Document> docs = ((MaterializedDocuments) evt.getTableEntry(inputVar)).getValue();
//        this.streamDocs = new StreamDocuments(Flowable.fromIterable(this.materializedDocs.getValue()));
//        return executeStreamInputStreamOutput();
//    }
//
//
//
//    @Override
//    public StreamGraphData executeStreamInputStreamOutput() {
////        if (!input.isParallel()) {input = input.parallel();}
////        Stream<GraphElement> temp = input.map(s -> getNeighbor(s.tokens, this.distance)).flatMap(Collection::stream);
//        Publisher<GraphElement> temp = Flowable.fromPublisher(this.streamDocs.getValue())
//                .observeOn(Schedulers.computation())
//                .map(s -> getNeighbor(s.tokens, this.distance))
//                .flatMapIterable(item -> item);
//        return new StreamGraphData(mergeGraphData(temp));
//    }
//
//    @Override
//    public MaterializedGraphData materialize() {
//        return (MaterializedGraphData) materializeStreamEntry(this.inputStream);
//    }
//
//    private static Publisher<GraphElement> mergeGraphData(Publisher<GraphElement> graph) {
//        ConcurrentHashMap<GraphElement, GraphElement> mergedGraph = new ConcurrentHashMap<>();
//        Flowable.fromPublisher(graph).observeOn(Schedulers.computation()).forEach(i -> {if (mergedGraph.containsKey(i)) {
//            int count = (Integer) mergedGraph.get(i).getProperty("count");
//            mergedGraph.get(i).addProperty("count", count+1);
//        }
//        else {
//            mergedGraph.put(i, i); } });
//        return Flowable.fromIterable(mergedGraph.keySet());
//    }
//
//
//    private List<GraphElement> getNeighbor(List<String> tokens, Integer distance) {
//        List<GraphElement> edges = new ArrayList<>();
//        for(int i=0; i < tokens.size(); i++) {
//            String w = tokens.get(i);
//            if (!words.contains(w)) {
//                continue;
//            }
//            Node t1 = new Node("word", "value", w);
//            for (int j=i+1; j <=Math.min(tokens.size()-1, i+distance); j++) {
//                String wj = tokens.get(j);
//                if (!words.contains(wj)) {continue;}
//                Node t2 = new Node("word", "value", wj);
//                Edge e = new Edge("cooccur_"+distance, "count", 1);
//                e.setNodes(t1, t2, true);
//                edges.add(e);
//            }
//        }
//        return edges;
//    }
//
//
//    public static void main(String[] args) {
////        List<Document> docs = new ArrayList<>();
////        docs.add(new Document("1", "I am a beautiful girl"));
////        docs.add(new Document("2", "I am Xiuwen Zheng"));
//////        docs.add(new Document("3", "I am a beautiful girl"));
////        Flowable<Document> x = Flowable.fromIterable(docs).map(s -> {
////            s.setTokens(Arrays.asList(s.text.split(" ")));
////            return s;});
////        Flowable<List<GraphElement>> y = x.map(s -> getNeighbor(s.tokens, 3));
////        Flowable<GraphElement> z = y.flatMapIterable(item -> item);
////        Flowable<GraphElement> rsult = Flowable.fromPublisher(mergeGraphData(z));
////        List<GraphElement> l = rsult.toList().blockingGet();
////        for (GraphElement i : l) {
////            Edge thisedge = (Edge) i;
////            System.out.println((thisedge.getFirstNode().getProperty("value")));
////            System.out.println((thisedge.getSecondNode().getProperty("value")));
////            System.out.println(thisedge.getProperty("count"));
////        }
//    }
//}

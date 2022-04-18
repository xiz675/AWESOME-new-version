//package edu.sdsc.queryprocessing.executor.execution.executionunit.textoperations.topicmodel;
//
//import cc.mallet.pipe.*;
//import cc.mallet.pipe.iterator.ArrayIterator;
//import edu.sdsc.datatype.execution.Document;
//import edu.sdsc.datatype.execution.FunctionParameter;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.datatype.execution.TextMatrix;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.functions.functionexecutionbase.FunctionInputStreamExecutionBase;
//import edu.sdsc.queryprocessing.executor.utils.StreamInstanceIterator;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.LDAPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import org.ejml.data.DMatrixRMaj;
//import cc.mallet.types.InstanceList;
//
//
//import cc.mallet.types.*;
//import cc.mallet.topics.*;
//import org.reactivestreams.Publisher;
//
//import java.util.*;
//import java.io.*;
//
//import java.util.List;
//import java.util.regex.Pattern;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//import static org.ejml.dense.row.CommonOps_DDRM.transpose;
//
//public class ExeLDA extends FunctionInputStreamExecutionBase {
//    private ExecutionVariableTable evt;
//    private Integer topicNum = 50;
////    private MaterializedDocuments doc;
//    private Publisher<Document> streamDocuments;
//    private List<Document> docData = new ArrayList<>();
//
//    public ExeLDA(LDAPhysical ope) {
////        this.parameters = translateParameters(this.ope.getParameters());
//        // get the documents varID
////        Pair<Integer, String> docID = ope.getDocID();
//        FunctionParameter num = ope.getNum();
//        if (num != null) {
//            if (num.isVar()) {
//                this.topicNum = (Integer) this.evt.getTableEntry(new Pair<>(num.getVarID(), "*")).getValue();
//            }
//            else {
//                this.topicNum = (Integer) num.getValue();
//            }
//        }
//    }
//
//
//
////    public ExeLDA(LDAPhysical ope, VariableTable vte, ExecutionVariableTable evt) {
////        this.evt = evt;
//////        this.parameters = translateParameters(this.ope.getParameters());
////        // get the documents varID
////        Pair<Integer, String> docID = ope.getDocID();
////        FunctionParameter num = ope.getNum();
////        if (num != null) {
////            if (num.isVar()) {
////                this.topicNum = (Integer) this.evt.getTableEntry(new Pair<>(num.getVarID(), "*")).getValue();
////            }
////            else {
////                this.topicNum = (Integer) num.getValue();
////            }
////        }
////        PipelineMode mode = ope.getExecutionMode();
////        this.setExecutionMode(mode);
////        if (mode.equals(PipelineMode.streaminput)) {
////            this.streamDocuments = ((StreamDocuments) evt.getTableEntry(docID)).getValue();
////        }
////        else {
////            assert mode.equals(PipelineMode.block);
////            this.docData = ((MaterializedDocuments) evt.getTableEntry(docID)).getValue();
////        }
////    }
//
//    @Override
//    // all parameters are either constant or can be extracted from execution table
//    // todo: add docID later, since it needs block; add check is token is null
//    public List<ExecutionTableEntryMaterialized> execute() {
////        MaterializedList<Document> docs = (MaterializedList<Document>) this.evt.getTableEntry(new Pair<>(parameters.get(0).getVarID(), "*"));
//        System.out.println("LDA input size: " + this.docData.size());
//
//        // apply streamInput so that it has to cost more time
//        this.streamDocuments = Flowable.fromIterable(this.docData);
//        return executeStreamInput();
////        List<String> docContent = new ArrayList<>();
//////        List<Object> docIDs = new ArrayList<>();
////        for (Document doc : this.docData) {
//////            if (doc.docID != null) {
//////                docIDs.add(doc.docID);
//////            }
////            if (doc.tokens != null) {
////                docContent.add(String.join(" ", doc.tokens));
////            }
////            else {
////                docContent.add(doc.text);
////            }
////        }
////        ArrayIterator docIter = new ArrayIterator(docContent);
////        ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
////        pipeList.add( new CharSequenceLowercase() );
////        pipeList.add( new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) );
//////        pipeList.add( new TokenSequenceRemoveStopwords(new File("stoplists/en.txt"), "UTF-8", false, false, false) );
////        pipeList.add( new TokenSequence2FeatureSequence() );
////        InstanceList instances = new InstanceList (new SerialPipes(pipeList));
//////        InstanceList instances = new InstanceList (new SerialPipes());
////        instances.addThruPipe(docIter);
////        // return runLDA(instances, topicNum);
////        if (this.docID.size() == docContent.size()) {
////            return runLDA(instances, topicNum, docIDs);
////        }
////        else { return runLDA(instances, topicNum);}
//    }
//
//    @Override
//    // the input of the stream input execution should be the main stream variables and the other parameters are either
//    // constant or can be extracted from execution table
//    // pass input to constructor
//    public List<ExecutionTableEntryMaterialized> executeStreamInput() {
//        Flowable<Document> x = Flowable.fromPublisher(streamDocuments);
//        // should change the iterator and make it returns contents
//        FlowableInstanceIterator flowIter = new FlowableInstanceIterator(x);
//        ArrayList<Pipe> pipeList = new ArrayList<Pipe>();
//        pipeList.add( new CharSequenceLowercase() );
//        pipeList.add( new CharSequence2TokenSequence(Pattern.compile("\\p{L}[\\p{L}\\p{P}]+\\p{L}")) );
////        pipeList.add( new TokenSequenceRemoveStopwords(new File("stoplists/en.txt"), "UTF-8", false, false, false) );
//        pipeList.add( new TokenSequence2FeatureSequence() );
//        InstanceList instances = new InstanceList (new SerialPipes(pipeList));
//        instances.addThruPipe(flowIter);
//        return runLDA(instances, topicNum);
//    }
//
//
//    // todo: should add docID here. If there is no docID, then should assign one, otherwise should use the user-assigned one
//    private List<ExecutionTableEntryMaterialized> runLDA(InstanceList instances, Integer num, List<Object>... docID) {
//        TextMatrix docTopicMatrix = new TextMatrix();
//        TextMatrix wordTopicMatrix = new TextMatrix();
//        ParallelTopicModel model = new ParallelTopicModel(num, 1.0, 0.01);
//        model.setRandomSeed(2);
//        model.addInstances(instances);
//        // Use two parallel samplers, which each look at one half the corpus and combine
//        //  statistics after every iteration.
//        model.setNumThreads(16);
//
//        // Run the model for 50 iterations and stop (this is for testing only,
//        //  for real applications, use 1000 to 2000 iterations)
//        model.setNumIterations(1000);
////        model.setNumIterations(50);
//        try {
//            model.estimate();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//
//        // Show the words and topics in the first instance
//
//        // The data alphabet maps word IDs to strings
//        Alphabet dataAlphabet = instances.getDataAlphabet();
//        List<String> words = new ArrayList<>();
//        for (int i = 0; i < dataAlphabet.size(); i++) {
//            words.add((String) dataAlphabet.lookupObject(i));
//        }
//        wordTopicMatrix.setRowMapping(words);
//        List<Integer> topicIDs = IntStream.rangeClosed(0, num-1)
//                .boxed().collect(Collectors.toList());
//        wordTopicMatrix.setColMapping(topicIDs);
//        docTopicMatrix.setColMapping(topicIDs);
//        List<TopicAssignment> result = model.getData();
//        if (docID.length == 0) {
//            docTopicMatrix.setRowMapping(IntStream.rangeClosed(0, result.size())
//                    .boxed().collect(Collectors.toList()));
//        }
//        else {
//            docTopicMatrix.setRowMapping(docID[0]);
//        }
////        FeatureSequence tokens = (FeatureSequence) model.getData().get(0).instance.getData();
////        LabelSequence topics = model.getData().get(0).topicSequence;
//        // set value for docTopicMatrix
//        // result size should be the num of documents
//        double[][] topicDis = new double[result.size()][num];
//        for (int i=0; i<result.size(); i++) {
//            topicDis[i] = model.getTopicProbabilities(i);
//        }
//        docTopicMatrix.setValue(new DMatrixRMaj(topicDis));
//        // get weights for top words
//        double[][] wordsWeight = model.getTopicWords(true, false);
//        DMatrixRMaj x = new DMatrixRMaj(wordsWeight);
//        DMatrixRMaj y = new DMatrixRMaj(x.numCols, x.numRows);
//        transpose(x, y);
//        wordTopicMatrix.setValue(y);
//        // add two matrices to executionTable
////        AwesomeTextMatrix<Integer, Integer> var1 = new AwesomeTextMatrix<Integer, Integer>(docTopicMatrix);
////        AwesomeTextMatrix<String, Integer> var2 = new AwesomeTextMatrix<String, Integer>(wordTopicMatrix);
//        AwesomeTextMatrix var1 = new AwesomeTextMatrix(docTopicMatrix);
//        AwesomeTextMatrix var2 = new AwesomeTextMatrix(wordTopicMatrix);
//        return new ArrayList<>() {{ add(var1); add(var2);}};
////        Pair<Integer, String> var1ID = ope.getOutputVar().iterator().next();
////        Pair<Integer, String> var2ID = ope.getOutputVar().iterator().next();
////        evt.insertEntry(var1ID, var1);
////        evt.insertEntry(var2ID, var2);
//    }
//
//    public void setDocData(List<Document> docData) {
//        this.docData = docData;
//    }
//}

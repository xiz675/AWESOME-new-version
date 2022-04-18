//package edu.sdsc.queryprocessing.executor.execution.executionunit.textoperations;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.Document;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.PipelineExeutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.textoperators.CreateDocumentsPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.RDBMSUtils;
//import io.reactivex.rxjava3.core.Flowable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//import org.jooq.DSLContext;
//import org.jooq.SQLDialect;
//import org.jooq.impl.DSL;
//import org.reactivestreams.Publisher;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//
//import static edu.sdsc.queryprocessing.executor.utils.MaterializeStream.materializeStreamEntry;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class ExeCreateDocuments extends PipelineExeutionBase {
////    private RDBMSUtils db_util;
//    private String rName;
//    private String textColName;
//    private String docIDColName;
//    private StreamDocuments streamResult;
//    private StreamRelation streamInput;
//    private Connection sqlCon;
//    private Integer threadsNum = 1;
//
//    // the get column operation is already incorporated here
//    public ExeCreateDocuments(JsonObject config, CreateDocumentsPhysical ope, ExecutionVariableTable evt, Connection sqlCon, Integer threadsNum, ExecutionVariableTable... localEvt) {
//        this.threadsNum = threadsNum;
//        String loc = ope.getExecuteUnit();
//        if (loc.equals("*")) {
//            this.sqlCon = sqlCon;
//        }
//        else {
//            RDBMSUtils db_util = new RDBMSUtils(config, loc);
//            this.sqlCon = db_util.getConnection();
//        }
//        this.rName = ope.rName;
//        this.docIDColName = ope.docIDCol;
//        this.textColName = ope.textColName;
////        this.sqlCon = sqlCon;
//        // set mode and assign the proper value
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        // for materialize, the input stream is only the streamInput
//        if (mode.equals(PipelineMode.materializestream)) {
//            this.streamResult = (StreamDocuments)  getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else if (mode.equals(PipelineMode.streaminput) || mode.equals(PipelineMode.pipeline)){
//            this.streamInput = (StreamRelation)  getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//    }
//
//    private Publisher<AwesomeRecord> getSQLResult() {
////        Connection conn = db_util.getConnection();
//        DSLContext create = DSL.using(sqlCon, SQLDialect.SQLITE);
//        String sql = String.format("select %s, %s from %s", docIDColName, textColName, rName);
//        // for debug use
////        List<AwesomeRecord> x = Flowable.fromPublisher(create.resultQuery(sql)).map(AwesomeRecord::new).toList().blockingGet();
////        System.out.println(x.get(0).getColumn(textColName));
//        return Flowable.fromPublisher(create.resultQuery(sql)).map(AwesomeRecord::new);
//    }
//
//    @Override
//    public MaterializedDocuments execute() {
//        streamResult = executeStreamOutput();
//        return materialize();
////        Publisher<Document> docsStream = executeStreamInputStreamOutput(new StreamRelation(getSQLResult())).getValue();
////        return new MaterializedDocuments(Flowable.fromPublisher(docsStream).toList().blockingGet());
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, docs);
//    }
//
//
//    @Override
//    public MaterializedDocuments executeStreamInput() {
//        return (MaterializedDocuments) materializeStreamEntry(executeStreamInputStreamOutput());
////        List<Document> rsult = Flowable.fromPublisher(executeStreamInputStreamOutput(input)).toList().blockingGet();
////        return new MaterializedDocuments(rsult);
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, docs);
//    }
//
//    @Override
//    public StreamDocuments executeStreamOutput() {
//        Publisher<AwesomeRecord> temp = getSQLResult();
//        this.streamInput = new StreamRelation(temp);
////        Flowable.fromPublisher(this.streamInput.getValue());
//        return executeStreamInputStreamOutput();
//    }
////        Flowable x = Flowable.fromPublisher(this.streamInput.getValue());
//
//
//    @Override
//    public StreamDocuments executeStreamInputStreamOutput() {
//        // todo: this should be changed, should support detecting the type of column and cast it to a Java type
////        parallel().runOn(Schedulers.computation())
//        return new StreamDocuments(Flowable.fromPublisher(streamInput.getValue()).replay().autoConnect().observeOn(Schedulers.computation()).map(s ->
//                new Document(s.getColumn(docIDColName), (String) s.getColumn(textColName))));
////        if (!input.isParallel()) {input = input.parallel();}
////        return input.map(s ->
////             new Document((String) s.getValue(docIDColName), (String) s.getValue(textColName)));
//    }
//
//    @Override
//    public MaterializedDocuments materialize() {
//        return (MaterializedDocuments) materializeStreamEntry(streamResult);
//    }
//
////    public Stream<Document>
////
////    public void execute() {
////        ExecutionDocuments docs = new ExecutionDocuments();
////        Stream<Document> docIDPair = loadPairDoc();
////
////    }
////
////    private Stream<Document> loadPairDoc() {
////
////    }
//
//}

//package edu.sdsc.queryprocessing.executor.execution.executionunit.relationoperations;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.PipelineExeutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.GetColumnPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import org.jooq.Record;
//import org.jooq.ResultQuery;
//import org.reactivestreams.Publisher;
//
//import javax.json.JsonObject;
//import java.sql.*;
//import java.util.List;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.queryprocessing.executor.utils.SQLiteQueryUtil.getSQLResult;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.isInLocal;
//
//public class GetColumn extends PipelineExeutionBase {
//    private String rName;
//    private String cName;
//    private StreamRelation streamInput;
//    private StreamList<Object> streamResult;
//    private List<AwesomeRecord> materializedValue;
//    private Connection sqlCon;
//
////    private Publisher<AwesomeRecord> relation;
//
//    // there are actually only two types of input. One is a SQLite relation and the other one is Publisher
//    // and the name can be provided or from the operator
//    // change the ColumnToList type to getColumn
//    // todo: if the input is local, then it is not in the input variable attribute and should get it from another attribute
//    public GetColumn(JsonObject config, GetColumnPhysical ope, ExecutionVariableTable evt, Connection sqlCon, ExecutionVariableTable... localEvt) {
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        this.sqlCon = sqlCon;
//        this.cName = ope.getColName();
//        Pair<Integer, String> inputVar = ope.getRelationID();
////        Pair<Integer, String> inputVar = ope.getInput Var().iterator().next();
//        if (mode.equals(PipelineMode.streamoutput) || mode.equals(PipelineMode.block)) {
//            this.rName = ope.getrName();
////            this.con = ParserUtil.sqliteConnect(config);
////            this.docs = ((MaterializedDocuments) evt.getTableEntry(inputVar));
//            // if this is a local relation, then needs to get the actual name as rName
//            MaterializedRelation tempValue = (MaterializedRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//            this.materializedValue = tempValue.getValue();
////                this.rName = tempValue.getValue().second;
//        }
//        else if (mode.equals(PipelineMode.materializestream)) {
//            this.streamResult = (StreamList) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else {
//            this.streamInput = (StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//    }
//
////    public GetColumn(JsonObject config, String rName, String cName, VariableTable vte) {
////
////    }
//
//    // for sub-operator, the output name is provided instaed of getting from the operator
//    // since the relation will be provided as input when calling the streaming executors, so there is no need for this constructor
////    public GetColumnPhysical(JsonObject config,  Publisher<AwesomeRecord> relation, String cName, VariableTable vte) {
////        this.cName = cName;
////        this.relation = relation;
////        this.con = ParserUtil.sqliteConnect(config);
////    }
//
//
//
//
//
//    @Override
//    public MaterializedList<Object> execute() {
////        StreamList<Object> result = executeStreamOutput();
//        List<Object> t = this.materializedValue.stream().map(i -> i.getColumn(cName)).collect(Collectors.toList());
//        for (Object i : t) {
//            System.out.println((String) i);
//        }
//        return new MaterializedList<>(t);
////        return new MaterializedList<>(Flowable.fromPublisher(result.getValue()).toList().blockingGet());
//    }
//
//    @Override
//    public MaterializedList<Object> executeStreamInput() {
//        return new MaterializedList<>(Flowable.fromPublisher(executeStreamInputStreamOutput().getValue()).toList().blockingGet());
//    }
//
//
//    // needs to run a sql query on sqlite to get the results
//    @Override
//    public StreamList<Object> executeStreamOutput() {
//        //            if (stmt == null) {
////                stmt = con.createStatement();
////            }
////        DSLContext create = DSL.using(con, SQLDialect.SQLITE);
////        String sql = String.format("select %s from %s", cName, rName);
//        ResultQuery<Record> result = getSQLResult(sqlCon, rName, cName);
//        Publisher<Object> flowableResult = Flowable.fromPublisher(result).map(i -> i.get(cName));
////        Publisher<Object> flowableResult = Flowable.fromPublisher(result).observeOn(Schedulers.computation()).map(i -> i.get(cName));
//        return new StreamList<>(flowableResult);
////            if (metaData.getColumnCount() != 1) {
////                throw new RuntimeException("Only one column is allowed.");
////            }
////            resultSet
////
////            while (resultSet.next()) {
////                if (metaData.getColumnTypeName(1).equals("INTEGER")) {
////                    result.add(resultSet.getInt(cName));
////                } else {
////                    result.add(resultSet.getString(cName));
////                }
////            }
////            return result;
//    }
//
//    @Override
//    public StreamList<Object> executeStreamInputStreamOutput() {
////        return new StreamList<>(Flowable.fromPublisher(this.streamInput.getValue()).observeOn(Schedulers.computation()).map(i -> i.getColumn(cName)));
//        return new StreamList<>(Flowable.fromPublisher(this.streamInput.getValue()).map(i -> i.getColumn(cName)));
//
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized materialize() {
//        return new MaterializedList<>(Flowable.fromPublisher((this.streamResult.getValue())).toList().blockingGet());
//    }
//}

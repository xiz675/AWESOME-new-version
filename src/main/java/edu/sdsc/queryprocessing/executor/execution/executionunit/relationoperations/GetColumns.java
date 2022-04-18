//package edu.sdsc.queryprocessing.executor.execution.ExecutionUnit.relationoperations;
//
//import edu.sdsc.queryprocessing.executor.execution.ElementVariable.*;
//import edu.sdsc.queryprocessing.executor.execution.ExecutionUnit.PipelineExeutionBase;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import org.jooq.DSLContext;
//import org.jooq.Record;
//import org.jooq.ResultQuery;
//import org.jooq.SQLDialect;
//import org.jooq.impl.DSL;
//import org.reactivestreams.Publisher;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.List;
//
//public class GetColumns implements PipelineExeutionBase {
//    private String rName;
//    private List<String> cNames;
//    private Connection con;
//    private ExecutionVariableTable evt;
//    private Statement stmt;
////    private Publisher<AwesomeRecord> relation;
//
//    // there are actually only two types of input. One is a SQLite relation and the other one is Publisher
//    // and the name can be provided or from the operator
//    // change the ColumnToList type to getColumn
//    public GetColumns(JsonObject config, String rName, List<String> cNames, VariableTable vte) {
//        this.cNames = cNames;
//        this.rName = rName;
//        this.con = ParserUtil.sqliteConnect(config);
//    }
//
//
//    @Override
//    public MaterializedList<Object> execute() {
//        StreamList<Object> result = executeStreamOutput();
//        return new MaterializedList<>(Flowable.fromPublisher(result.getValue()).toList().blockingGet());
//    }
//
//    @Override
//    public MaterializedList<Object> executeStreamInput(ExecutionTableEntryStream input) {
//        return new MaterializedList<>(Flowable.fromPublisher(executeStreamInputStreamOutput(input).getValue()).toList().blockingGet());
//    }
//
//
//    // needs to run a sql query on sqlite to get the results
//    @Override
//    public StreamList<Object> executeStreamOutput() {
//        try {
//            if (stmt == null) {
//                stmt = con.createStatement();
//            }
//            DSLContext create = DSL.using(con, SQLDialect.SQLITE);
//
//            String sql = String.format("select %s from %s", cName, rName);
//            ResultQuery<Record> result = create.resultQuery(sql);
//            Publisher<Object> flowableResult = Flowable.fromPublisher(result).map(i -> i.get(cName));
//            return new StreamList<>(flowableResult);
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
//        }  catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return null;
//    }
//
//    @Override
//    public StreamList<Object> executeStreamInputStreamOutput(ExecutionTableEntryStream input) {
//        return new StreamList<>(Flowable.fromPublisher(((StreamRelation) input).getValue()).map(i -> i.getColumn(cName)));
//    }
//
//    @Override
//    public ExecutionTableEntryMaterialized materialize(ExecutionTableEntryStream input) {
//        return new MaterializedList<>(Flowable.fromPublisher(((StreamList<Object>) input).getValue()).toList().blockingGet());
//    }
//}

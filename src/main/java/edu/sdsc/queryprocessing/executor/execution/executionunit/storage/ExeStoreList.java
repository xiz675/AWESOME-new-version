//
//package edu.sdsc.queryprocessing.executor.execution.executionunit.storage;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.InputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.storage.StoreList;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.RDBMSUtils;
//import org.reactivestreams.Publisher;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//import static edu.sdsc.utils.RelationUtil.*;
//
//public class ExeStoreList extends InputStreamExecutionBase {
//    // this is for materialized List and HL
//    private List<Object> materializedList = new ArrayList<Object>();
//    // this is for stream list
//    private Publisher streamList;
//    // this is for nested list
////    private Publisher<ExecutionTableEntry> streamHL;
//    private boolean isHL = false;
//    private boolean isNested = false;
//    private String tableName;
//    //    private boolean rowIndex;
////    private boolean columnIndex;
//    private boolean index;
//    private List<String> columnNames;
//    private Connection toCon;
//    private RDBMSUtils rDBMS;
//
//    //  this can be a HLCollection type or a list type. For HLCollection, needs to determine the input type
//    //  which can be double, String, Integer, List  (all of the elements are materialized)
//    public ExeStoreList(JsonObject config, StoreList store, ExecutionVariableTable evt, Connection sqlCon, ExecutionVariableTable... localEvt) {
//        String dbName = store.getDbName();
//        if (dbName.equals("*")) {
//            this.toCon = sqlCon;
//        }
//        else {
//            RDBMSUtils db_util = new RDBMSUtils(config, dbName);
//            this.toCon = db_util.getConnection();
//        }
//        this.columnNames = store.getColumnNames();
//        this.index = store.isIndex();
//        this.tableName = store.getTableName();
//        this.rDBMS = new RDBMSUtils(config, dbName);
//        PipelineMode mode = store.getExecutionMode();
//        this.setExecutionMode(mode);
//        Pair<Integer, String> storeVar = store.getStoreVarID();
//        if (mode.equals(PipelineMode.streaminput)) {
//            // for streamHL, can't know if the element is a list or not
//            ExecutionTableEntry temp = getTableEntryWithLocal(storeVar, evt, localEvt);
//            if (temp instanceof StreamList) {
//                this.streamList = ((StreamList) temp).getValue();
//            }
//            else {
//                assert temp instanceof StreamHLCollection;
//                this.isHL = true;
//                this.streamList = ((StreamHLCollection) temp).getValue();
//            }
//        }
//        else {
//            ExecutionTableEntry temp = getTableEntryWithLocal(storeVar, evt, localEvt);
//            if (temp instanceof MaterializedList) {
//                this.materializedList = (List) temp.getValue();
//                if (this.materializedList.size() > 0) {
//                    Object x = this.materializedList.get(0);
//                    if (x instanceof List) {
//                        isNested = true;
//                    }
//                }
//            }
//            else {
//                assert temp instanceof MaterializedHLCollection;
//                this.isHL = true;
//                List<ExecutionTableEntry> TEs = ((MaterializedHLCollection) temp).getValue();
//                if (TEs.size() > 0) {
//                    ExecutionTableEntry first = TEs.get(0);
//                    assert first instanceof MaterializedList || first instanceof AwesomeDouble ||
//                            first instanceof AwesomeInteger || first instanceof AwesomeString;
//                    // if this is a list of list
//                    if (first instanceof MaterializedList) {
//                        isNested = true;
//                    }
//                    for (ExecutionTableEntry TE : TEs) {
//                        this.materializedList.add(TE.getValue());
//                    }
//                }
//            }
//        }
//    }
//
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        try {
//            if (rDBMS.isTableInDB(tableName)) {
//                rDBMS.deleteTable(tableName);
//            }
//            createTable(toCon, tableName, columnNames);
//            insertListToTable(materializedList, toCon, tableName, columnNames, index, isNested);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return new VoidTableEntry();
//    }
//
//
//    @Override
//    // it can be a stream of HL or a stream of primitive which includes lists and scalar types, since it is a stream, can't know
//    // if the inside is a list or not at this stage
//    public ExecutionTableEntryMaterialized executeStreamInput() {
//        try {
//            if (rDBMS.isTableInDB(tableName)) {
//                rDBMS.deleteTable(tableName);
//            }
//            createTable(toCon, tableName, columnNames);
//            insertStreamToTable(streamList, toCon, tableName, columnNames, index, isHL);
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return new VoidTableEntry();
//    }
//}
//*/

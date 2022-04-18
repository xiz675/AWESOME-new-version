//package edu.sdsc.queryprocessing.executor.execution.executionunit.highleveloperations;
//
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.datatype.execution.storetosqlite.ResultToStore;
//import edu.sdsc.datatype.execution.TextMatrix;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.ExecuteSQLPhysical;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.PageRankPhysical;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.variables.logicalvariables.DataTypeEnum;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import io.reactivex.rxjava3.parallel.ParallelFlowable;
//import io.reactivex.rxjava3.schedulers.Schedulers;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//
//import static edu.sdsc.queryprocessing.executor.utils.ExecutionUtil.*;
//import static edu.sdsc.queryprocessing.planner.physicalplan.createplan.CreatePhysicalPlan.findOpeByID;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class MapExecutor {
//    private PipelineMode executionMode;
//    private StreamHLCollection streamTEs;
//    private MaterializedHLCollection listTEs;
//    private StreamList<Object> streamData;
//    private MaterializedList<Object> listData;
//    private StreamHLCollection streamResult;
//    private boolean isTE = false;
//    private Pair<Integer, String> localVar;
//    private ExecutionVariableTable evt;
//    private VariableTable vt;
//    private PhysicalOperator innerOpe;
//    private JsonObject config;
//    private DataTypeEnum elementType;
//    private List<PhysicalOperator> physicalGraph;
//    private Connection sqlCon;
//    private boolean parallelTask;
//
//    // todo: add other execution methods later
//    // todo: find the inner operator node from graph by its id
//    public MapExecutor(MapPhysical ope,  ExecutionVariableTable evt, VariableTable vt, JsonObject config, List<PhysicalOperator> g, Connection sqlCon, boolean taskParallel, ExecutionVariableTable... localEvt) {
//        // assign the value based on the type of the table entry
//        this.innerOpe = findOpeByID(g, ope.getInputOperator().iterator().next());
////        this.innerOpe = innerOpe;
//        this.parallelTask = taskParallel;
//        this.physicalGraph = g;
//        this.config = config;
//        this.elementType = ope.getElementType();
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        Pair<Integer, String> inputVar = new Pair<>(ope.getAppliedVarID(), "*");
//        ExecutionTableEntry input = getTableEntryWithLocal(inputVar, evt, localEvt);
//        this.localVar = new Pair<>(ope.getLocalVarID().get(0), "*");
//        this.evt = evt;
//        this.vt = vt;
//        this.sqlCon = sqlCon;
//        if (mode.equals(PipelineMode.materializestream)) {
//            this.streamResult = (StreamHLCollection) input;
//        }
//        else if (mode.equals(PipelineMode.pipeline) || mode.equals(PipelineMode.streaminput)){
//            if (input instanceof StreamHLCollection) {
//                this.streamTEs = (StreamHLCollection) input;
//                this.isTE = true;
//            }
//            else {
//                assert input instanceof StreamList;
//                this.streamData = (StreamList) input;
//            }
//        }
//        else {
//            if (input instanceof MaterializedHLCollection) {
//                this.listTEs = (MaterializedHLCollection) input;
//                this.isTE = true;
//            }
//            else {this.listData = (MaterializedList) input;}
//        }
//    }
//
//
//    public PipelineMode getExecutionMode() {
//        return executionMode;
//    }
//
//    public void setExecutionMode(PipelineMode executionMode) {
//        this.executionMode = executionMode;
//    }
//
//    public ExecutionTableEntry executeWithMode() {
//        ExecutionTableEntry result;
//        long startTime;
//        long endTime;
//        if (this.executionMode.equals(PipelineMode.block)) {
//            startTime = System.currentTimeMillis();
//            result = this.execute();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else if (this.executionMode.equals(PipelineMode.streaminput)) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else if (this.executionMode.equals(PipelineMode.materializestream)) {
//            startTime = System.currentTimeMillis();
//            result = this.materialize();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else if (this.executionMode.equals(PipelineMode.streamoutput)) {
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamOutput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//        }
//        else {
//            assert this.executionMode.equals(PipelineMode.pipeline);
//            startTime = System.currentTimeMillis();
//            result = this.executeStreamInputStreamOutput();
//            endTime = System.currentTimeMillis();
//            System.out.println(this.getClass().getSimpleName() + ":" + (endTime - startTime) + " ms");
//
//        }
//        return result;
//    }
//
//
//    // for any type of input, needs to determine if this is a TE or not
//    // instead of inserting to the global ExecutionTableEntry, should have a separate local one for local variables
//    public MaterializedHLCollection execute() {
//        return new MaterializedHLCollection(Flowable.fromPublisher(executeStreamOutput().getValue()).toList().blockingGet());
//    }
//
//    // for each element, need to call forEach on it. materialize result from executeStreamInput().
//    public MaterializedHLCollection executeStreamInput() {
//        return new MaterializedHLCollection(Flowable.fromPublisher(executeStreamInputStreamOutput().getValue()).toList().blockingGet());
//    }
//
//    public MaterializedHLCollection materialize() {
//        return null;
//    }
//
//    public StreamHLCollection executeStreamOutput() {
//        if (isTE) {
//            List<ExecutionTableEntry> TEs = this.listTEs.getValue();
//            this.streamTEs = new StreamHLCollection(Flowable.fromIterable(TEs));
//        }
//        else {
//            List<Object> data = listData.getValue();
//            this.streamData = new StreamList<>(Flowable.fromIterable(data));
//        }
//        return executeStreamInputStreamOutput();
//    }
//
//    // every other execution functions can call it
//    public StreamHLCollection executeStreamInputStreamOutput() {
//        Flowable<ExecutionTableEntry> result;
//        // if the input is a List created by HLOs
//        if (isTE) {
//            // this should be flowable of table entries
//            Flowable<ExecutionTableEntry> streamTEs = Flowable.fromPublisher(this.streamTEs.getValue()).replay().autoConnect().subscribeOn(Schedulers.computation());
//            if (parallelTask) {
////                // for these operators, when the output is blocking which means it is stored to SQLite, needs to get the blocking part out
////                if ((innerOpe.getExecutionMode().equals(PipelineMode.block) || innerOpe.getExecutionMode().equals(PipelineMode.materializestream)) &&
////                        ((innerOpe instanceof ExecuteCypherPhysical) || (innerOpe instanceof ExecuteSQLPhysical) || (innerOpe instanceof PageRankPhysical))) {
////                    ParallelFlowable<ResultToStore> resultStream = streamTEs.parallel().runOn(Schedulers.computation()).map(i -> {
////                        ExecutionVariableTable localEvt = new ExecutionVariableTable();
////                        localEvt.insertEntry(this.localVar, i);
////                        //                this.evt.insertEntry(localVar, crtTE);
////                        return getResultToStore(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
////                    });
////                    // get the Result to store and then initiate sequential SQLite access
////                    List<ResultToStore> resultList = resultStream.sequential().toList().blockingGet();
////                    Long startTime = System.currentTimeMillis();
////                    result = store2SQLiteSequ(resultList, sqlCon);
////                    Long endTime = System.currentTimeMillis();
////                    System.out.println("storeToSQLite" + ":" + (endTime - startTime) + " ms");
////                    // should return list of AwsmRelation TE
////                }
////                // else, all parts can be done in parallel
////                else {
//                    ParallelFlowable<ExecutionTableEntry> resultStream = streamTEs.parallel().runOn(Schedulers.computation()).map(i -> {
//                        ExecutionVariableTable localEvt = new ExecutionVariableTable();
//                        localEvt.insertEntry(this.localVar, i);
////                this.evt.insertEntry(this.localVar, i);
//                        return getTableEntry(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//                    });
//                    result = resultStream.sequential();
////                        .toList().blockingGet();
//            }
//            else {
//                // if not parallel, just execute all of them
//                result = streamTEs.replay().autoConnect().observeOn(Schedulers.computation()).map( i -> {
//                    System.out.println("Map:" + Thread.currentThread().getName());
//                    ExecutionVariableTable localEvt = new ExecutionVariableTable();
//                    localEvt.insertEntry(this.localVar, i);
////                this.evt.insertEntry(this.localVar, i);
//                    return getTableEntry(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);}
//                );
//            }
//        }
//        else {
//            // this is flowable of values and should create table entry for the values
//            Flowable<Object> streamR = Flowable.fromPublisher(this.streamData.getValue()).replay().autoConnect().subscribeOn(Schedulers.computation());
//            if (parallelTask) {
//                // if the operator is execute query and the result is then stored in memory,
//                // then execute queries in parallel and store to SQLites in sequence
//                if ((innerOpe.getExecutionMode().equals(PipelineMode.block) || innerOpe.getExecutionMode().equals(PipelineMode.materializestream)) &&
//                        ((innerOpe instanceof ExecuteCypherPhysical) || (innerOpe instanceof ExecuteSQLPhysical) || (innerOpe instanceof PageRankPhysical))) {
//                    ParallelFlowable<ResultToStore> resultStream = streamR.parallel().runOn(Schedulers.computation()).map(i -> {
//                        ExecutionVariableTable localEvt = new ExecutionVariableTable();
//                        localEvt.insertEntry(this.localVar, createTableEntryForLocal(elementType, i));
//                        //                this.evt.insertEntry(localVar, crtTE);
//                        return getResultToStore(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//                    });
//                    // get the Results to store and then initiate sequential SQLite access
//                    List<ResultToStore> resultList = resultStream.sequential().toList().blockingGet();
//                    // should return list of AwsmRelation TEs
//                    result = store2SQLiteSequ(resultList, sqlCon);
//                }
//                else {
//                    ParallelFlowable<ExecutionTableEntry> resultStream = streamR.parallel().runOn(Schedulers.computation()).map(i -> {
//                        ExecutionVariableTable localEvt = new ExecutionVariableTable();
//                        localEvt.insertEntry(this.localVar, createTableEntryForLocal(elementType, i));
//                        //                this.evt.insertEntry(localVar, crtTE);
//                        return getTableEntry(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//                    });
//                    result = resultStream.sequential();
//    //                        .toList().blockingGet();
//                }
//            }
//            else {
//                result = streamR.replay().autoConnect().observeOn(Schedulers.computation()).map(i -> {
//                    System.out.println("Map:" + Thread.currentThread().getName());
//                    ExecutionVariableTable localEvt = new ExecutionVariableTable();
//                    localEvt.insertEntry(this.localVar, createTableEntryForLocal(elementType, i));
//                    //                this.evt.insertEntry(localVar, crtTE);
//                    return getTableEntry(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt); });
//            }
//        }
//        return new StreamHLCollection(result);
//    }
//
//
//
//
//    // todo: should add more types
//    // note that there is no way to create a list of graphs without a map creation. So in this case there can only be a list of integers or String, boolean etc.
//    // there should not be complex data types
//    public static ExecutionTableEntry createTableEntryForLocal(DataTypeEnum elementType, Object value) {
//        ExecutionTableEntry result;
//        if (elementType.equals(DataTypeEnum.Integer)) {
//            result = new AwesomeInteger((Integer) value);
//        }
//        else if (elementType.equals(DataTypeEnum.Double)) {
//            result = new AwesomeDouble((Double) value);
//        }
//        else if (elementType.equals(DataTypeEnum.Boolean)) {
//            result = new AwesomeBoolean((boolean) value);
//        }
//        else if (elementType.equals(DataTypeEnum.TextMatrix)) {
//            result = new AwesomeTextMatrix((TextMatrix) value);
//        }
//        else {
//            assert elementType.equals(DataTypeEnum.String);
//            result = new AwesomeString((String) value);
//        }
//        return result;
//    }
//
//    // todo: change this
//    private static ResultToStore getResultToStore(PhysicalOperator innerOpe, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
//                                                 List<PhysicalOperator> physicalGraph, Connection sqlCon, ExecutionVariableTable localEvt ) {
//        if (!innerOpe.isHasDependentOpe()){
//            System.out.println(Thread.currentThread().getName());
//            return singleOutputExecutionWithResultToStore(innerOpe, vt, config, evt, sqlCon, localEvt);
//        }
//        else {
//            // need to recursively execute dependent operator and insert them to table entry
//            return executeDependentOperatorsWithResultToStore(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//        }
//    }
//
//    private static Flowable<ExecutionTableEntry> store2SQLiteSequ(List<ResultToStore> resultList, Connection sqlCon) {
//        List<ExecutionTableEntry> result = new ArrayList<>();
//        for (ResultToStore i : resultList) {
//            // should add a function inside each ResultToStore
//            try {
//                i.storeToSQLite(sqlCon);
//            } catch (SQLException e) {
//                e.printStackTrace();
//            }
//            result.add(new MaterializedRelation(new Pair<>("*", i.getOuptStr())));
//        }
////        try {
////            toCon.close();
////        } catch (SQLException e) {
////            e.printStackTrace();
////        }
//        return Flowable.fromIterable(result);
//    }
//
//    private static ExecutionTableEntry getTableEntry(PhysicalOperator innerOpe, VariableTable vt, JsonObject config, ExecutionVariableTable evt,
//                                                     List<PhysicalOperator> physicalGraph, Connection sqlCon, ExecutionVariableTable localEvt) {
//        System.out.println(innerOpe.getClass() + ":" + Thread.currentThread().getName());
//        if (!innerOpe.isHasDependentOpe()){
//            return singleOutputExecution(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//        }
//        else {
//            // need to recursively execute dependent operator and insert them to table entry
//            return executeDependentOperators(innerOpe, vt, config, evt, physicalGraph, sqlCon, localEvt);
//        }
//    }
//}

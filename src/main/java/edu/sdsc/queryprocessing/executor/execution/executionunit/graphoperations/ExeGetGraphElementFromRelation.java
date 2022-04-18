//package edu.sdsc.queryprocessing.executor.execution.executionunit.graphoperations;
//
//import edu.sdsc.datatype.execution.*;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.PipelineExeutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromRelationPhysical;
//import edu.sdsc.utils.Pair;
//import io.reactivex.rxjava3.core.Flowable;
//import org.jooq.DSLContext;
//import org.jooq.SQLDialect;
//import org.jooq.impl.DSL;
//import org.reactivestreams.Publisher;
//
//import java.sql.Connection;
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.queryprocessing.executor.utils.MaterializeStream.materializeStreamEntry;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class ExeGetGraphElementFromRelation extends PipelineExeutionBase {
//    private BuildGraphFromRelationPhysical ope;
//    private Connection sqlCon;
//    private StreamRelation streamRelation;
//    private StreamGraphData streamResult;
//    private MaterializedRelation materializedRelation;
//
//    public ExeGetGraphElementFromRelation(BuildGraphFromRelationPhysical ope, ExecutionVariableTable evt, Connection sqlCon, ExecutionVariableTable... localEvt) {
//        // set variables based on execution mode.
//        // set mode and assign the proper value
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        // todo: currently the relation is in memory, but later if the relation is too large, then it should be stored. So should distinguish between in memory and sqlite/ postgres
//        //  should have a way to get the location of relation/graph
//        Pair<Integer, String> inputVar = new Pair<>(ope.getRelationID(), "*");
////        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        this.ope = ope;
//        if (mode.equals(PipelineMode.streamoutput) || mode.equals(PipelineMode.block)) {
//            this.materializedRelation = (MaterializedRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        // for materialize, the input stream is different
//        else if (mode.equals(PipelineMode.materializestream)) {
//            this.streamResult = (StreamGraphData) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else {
//            this.streamRelation = (StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        this.sqlCon = sqlCon;
//    }
//
//    @Override
//    public MaterializedGraphData materialize() {
//        return (MaterializedGraphData) materializeStreamEntry(this.streamResult);
//    }
//
//    @Override
//    public MaterializedGraphData execute() {
//        List<AwesomeRecord> result = this.materializedRelation.getValue();
//        return new MaterializedGraphData(result.stream().map(this::matchToEdge).collect(Collectors.toList()));
//
////
////        this.streamResult = executeStreamOutput();
////        return materialize();
////        List<GraphElement> rsult = Flowable.fromPublisher()).toList().blockingGet();
////        MaterializedGraphData docs = new MaterializedGraphData(rsult);
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, docs);
//    }
//
//
//    @Override
//    public MaterializedGraphData executeStreamInput() {
//        this.streamResult = executeStreamInputStreamOutput();
//        return materialize();
////        List<GraphElement> rsult = Flowable.fromPublisher(executeStreamInputStreamOutput(input)).toList().blockingGet();
//////        List<GraphElement> rsult = executeStreamInputStreamOutput(input).subscribe();
////////                collect(Collectors.toList());
////        MaterializedGraphData docs = new MaterializedGraphData(rsult);
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        evt.insertEntry(ouptVar, docs);
//    }
//
//    @Override
//    public StreamGraphData executeStreamOutput() {
////        Publisher<AwesomeRecord> docs = getSQLResult(this.materializedRelation.getValue().second);
////        this.streamRelation = new StreamRelation(docs);
////        return executeStreamInputStreamOutput();
//        return null;
//    }
//
////    @Override
////    public ExecutionTableEntryStream executeStreamInputStreamOutput() {
////        return null;
////    }
//
//
//    @Override
//    public StreamGraphData executeStreamInputStreamOutput() {
////        if (!input.isParallel()) {input = input.parallel();}
////        return new StreamGraphData(Flowable.fromPublisher((this.streamRelation.getValue())).observeOn(Schedulers.computation()).map((this::matchToEdge)));
//        return new StreamGraphData(Flowable.fromPublisher((this.streamRelation.getValue())).map((this::matchToEdge)));
//    }
//
//
//    private Publisher<AwesomeRecord> getSQLResult(String relationName) {
////        Connection conn = db_util.getConnection();
//        DSLContext create = DSL.using(this.sqlCon, SQLDialect.SQLITE);
//        // todo: split by "//." move this to logical part
//        Set<String> colNames = this.ope.getColNames();
////        List<String> splitedColName = new ArrayList<>();
////        for (String s : colNames) {
////            String[] ss = s.split("\\.");
////            if (ss.length == 0) {
////                splitedColName.add(s);
////            }
////            else {splitedColName.add(ss[1]);}
////        }
//        String joinedName = String.join(",", colNames);
//        String sql = String.format("select %s from %s", joinedName, relationName);
//        return Flowable.fromPublisher(create.resultQuery(sql)).map(AwesomeRecord::new);
//    }
//
//    private void addToPropertyWithValue(GraphElement n) {
//
//    }
//
//    private GraphElement matchToEdge(AwesomeRecord r) {
//        Set<String> colName = ope.getColNames();
//        Map<String, Object> colValues = new HashMap<>();
//        for (String col : colName) {
//            colValues.put(col, r.getColumn(col));
//        }
//        Map<String, String> node1Prop = ope.getNode1Property();
//        Map<String, String> node2Prop = ope.getNode1Property();
//        Map<String, String> edgeProp = ope.getNode1Property();
//        Node node1 = ope.getNode1();
//        Node node2 = ope.getNode2();
//        Edge edge = ope.getEdge();
//        for (String key: node1Prop.keySet()) {
//            node1.addProperty(key, colValues.get(node1Prop.get(key)));
//        }
//        for (String key: node2Prop.keySet()) {
//            node2.addProperty(key, colValues.get(node2Prop.get(key)));
//        }
//        for (String key: edgeProp.keySet()) {
//            node1.addProperty(key, colValues.get(edgeProp.get(key)));
//        }
//        edge.setNodes(node1, node2, ope.isHasDirection());
//        return edge;
//    }
//
//
//}

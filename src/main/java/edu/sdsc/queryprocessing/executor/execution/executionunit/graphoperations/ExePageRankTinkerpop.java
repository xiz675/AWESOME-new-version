//package edu.sdsc.queryprocessing.executor.execution.executionunit.graphoperations;
//
//import edu.sdsc.datatype.execution.*;
//import edu.sdsc.datatype.execution.storetosqlite.ResultToStore;
//import edu.sdsc.datatype.execution.storetosqlite.AwesomeRecordsToStore;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamRelation;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.PageRankPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.T;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
//
//import java.sql.Connection;
//import java.util.*;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.getParameterWithKey;
//import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//
//public class ExePageRankTinkerpop extends OutputStreamExecutionBase {
////    private List<FunctionParameter> parameters;
//    private Graph graph;
//    private boolean limitedNumber=false;
//    private Integer numOfNodes=null;
//    private String tableName=null;
//    private StreamRelation streamResult;
//    private Connection toCon;
//
//    // since it does not accept stream input, there is only one possible input (which is the materialized one)
//    // this constructor is for operators instead of sub-operator. So every parameter variable can be found from execution Table
//    public ExePageRankTinkerpop(PageRankPhysical ope, VariableTable vt, ExecutionVariableTable evt, Connection sqlCon, ExecutionVariableTable... localEvt) {
////        this.config = config;
////        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
////        this.tableName = "pagerank" + getRandomString(5);
//        // set mode
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        List<FunctionParameter> parameters = translateParameters(ope.getParameters());
//        FunctionParameter topk = getParameterWithKey(parameters, "topk");
//        if (topk != null) {
//            this.limitedNumber = (boolean) topk.getValueWithExecutionResult(evt, localEvt);
//            if (this.limitedNumber) {
//                this.numOfNodes = (Integer) getParameterWithKey(parameters, "num").getValueWithExecutionResult(evt, localEvt);
//            }
//        }
//        if (mode.equals(PipelineMode.materializestream)) {
//            // for materialize mode, the only input is the input stream
//            Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//            this.streamResult = (StreamRelation) getTableEntryWithLocal(inputVar, evt, localEvt);
//        }
//        else {
//            // only when the execution mode is not materialize, need to get the parameters
////            JsonArray parameterArray = ope.getParameters();
//            Integer graphID = parameters.get(0).getVarID();
//            this.graph = (Graph) getTableEntryWithLocal(new Pair<>(graphID, "*"), evt, localEvt).getValue();
//            Iterator<Vertex> s = this.graph.vertices();
//            int count = 0;
//            while (s.hasNext()) {
//                s.next();
//                count=count+1;
//            }
//            System.out.println("page rank on graph with size " + count);
//        }
//        this.toCon = sqlCon;
//    }
//
//    // this is sub-operation's constructor
////    public ExePageRankTinkerpop(Graph graph, boolean limitedNumber, Integer... num) {
////        this.graph = graph;
////        this.limitedNumber = limitedNumber;
////        if (this.limitedNumber) {
////            assert num.length == 1;
////            this.numOfNodes = num[0];
////        }
////    }
//
//    // for sub-operators which require blocking
////    public ExePageRankTinkerpop(JsonObject config, Graph graph, boolean limitedNumber,  String tableName, Integer... num) {
////        new ExePageRankTinkerpop(graph, limitedNumber, num);
////        this.config = config;
////        this.tableName = tableName;
////    }
//
//    // For unify them, return ExecutionXXX. For the result returned by Tinkerpop pagerank, needs to
//    // materialize it to SQLite
//    @Override
//    public MaterializedRelation execute() {
//        List<AwesomeRecord> queryResult = executePageRank();
////        List<String> cols = new ArrayList<>();
////        cols.add("id");
////        cols.add("pagerank");
//        return new MaterializedRelation(queryResult);
//
////        while (tempMap.hasNext()) {
////            Map<Object, Object> x = tempMap.next();
////            System.out.println(x.get(T.id));
////            System.out.println(x.get("gremlin.pageRankVertexProgram.pageRank"));
////        }
////        x.next();
//
//    }
//
//
//    @Override
//    public StreamRelation executeStreamOutput() {
//        return null;
//    }
//
//    // materialize will put it to a SQLite table
//    @Override
//    public MaterializedRelation materialize() {
//        return null;
////        List<String> cols = new ArrayList<>();
////        cols.add("id");
////        cols.add("pagerank");
//////        Connection toCon = ParserUtil.sqliteConnect(config);
////        createTable(toCon, tableName, cols);
////        insertToTable(streamResult.getValue(), toCon, tableName, cols);
//////        try {
//////            toCon.close();
//////        } catch (SQLException e) {
//////            e.printStackTrace();
//////        }
////        return new MaterializedRelation(new Pair<>("*", tableName));
////        er.setValue(new Pair<>("*", tableName));
////        return er;
//    }
//
//    public ResultToStore executeWithResult() {
//        List<AwesomeRecord> queryResult = executePageRank();
//        List<String> cols = new ArrayList<>();
//        cols.add("id");
//        cols.add("pagerank");
//        return new AwesomeRecordsToStore(queryResult, cols, tableName);
//    }
//
//    private List<AwesomeRecord> executePageRank() {
//        GraphTraversalSource g = graph.traversal().withComputer();
//        GraphTraversal<Vertex, Map<Object, Object>> tempMap;
//        if (limitedNumber) {
//            tempMap = g.V().pageRank().by("gremlin.pageRankVertexProgram.pageRank").limit(numOfNodes)
//                    .elementMap("gremlin.pageRankVertexProgram.pageRank");
//        }
//        else {
//            tempMap = g.V().pageRank().elementMap("gremlin.pageRankVertexProgram.pageRank");
//        }
//        try {
//            g.close();
//            graph.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//        return tempMap.toList().stream().map(i -> {Map<String, Object> j = new HashMap<>(); j.put("id", i.get(T.id));
//            j.put("pagerank", i.get("gremlin.pageRankVertexProgram.pageRank")); return j;}).map(AwesomeRecord::new).collect(Collectors.toList());
////        Iterable<Map<Object, Object>> iterableMap = getIterableFromIterator(tempMap);
////
////        iterableMap.
////
////        return Flowable.fromIterable(
//    }
//
//
//    public static void main(String[] args) {
//        Graph graph = TinkerFactory.createModern();
////        Graph graph = TinkerGraph.open(); //1.创建一个新的内存TinkerGraph并将其分配给该变量graph。
////        Vertex marko = graph.addVertex("name", "marko", "age", 29);
////        Vertex xiuwen = graph.addVertex("name", "xiuwen", "age", 24);
////        xiuwen.property("name", "xiuwen", "age", 24);
////        Vertex qiyu = graph.addVertex("Person");
////        qiyu.property("name", "qiyu");
////        marko.addEdge("like", xiuwen);
////        Vertex x = graph.addVertex("Person");
////        x.addEdge("like", xiuwen);
//        GraphTraversalSource g = graph.traversal().withComputer();
////        GraphTraversal<Vertex, Vertex> s = g.V().pageRank();
////        System.out.println(s.valueMap().toList());
////        while (s.hasNext()) {
////            Vertex ss = s.next();
////            System.out.println(ss.property("name"));
////            System.out.println(ss.id());
////        }
//        GraphTraversal<Vertex, Map<Object, Object>> tempMap = g.V().pageRank().elementMap("gremlin.pageRankVertexProgram.pageRank");
////        System.out.println(tempMap);
//        long start = System.currentTimeMillis();
//        tempMap.next();
//        System.out.println("first element: " + (System.currentTimeMillis() - start));
//        List x = tempMap.toList();
////            System.out.println(x.get(T.id));
////            System.out.println(x.get("gremlin.pageRankVertexProgram.pageRank"));
//        System.out.println("all elements: " + (System.currentTimeMillis() - start));
////        System.out.println(s.elementMap().toList());
////        System.out.println(s.id().toList());
////        Graph graph = TinkerFactory.createModern();
////        // compute pagerank
////        GraphTraversalSource g = graph.traversal().withComputer();
//
////        GraphTraversal<Vertex, Map<Object, Object>> x = g.V().valueMap();
////        while (x.hasNext()) {
////            Map v = x.next();
//////            System.out.println(v.id());
////            System.out.println(v.get("name"));
////        }
////        g.V().pageRank().valueMap()
////                System.out.println(x.valueMap("gremlin.pageRankVertexProgram.pageRank").toList());
//
//    }
//
//}
//

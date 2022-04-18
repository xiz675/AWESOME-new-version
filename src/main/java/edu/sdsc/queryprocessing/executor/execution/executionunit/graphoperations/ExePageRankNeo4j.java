//package edu.sdsc.queryprocessing.executor.execution.executionunit.graphoperations;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.FunctionParameter;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.FunctionPhysicalOperator;
//import edu.sdsc.utils.CypherUtils;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import org.neo4j.driver.Result;
//
//import javax.json.JsonArray;
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.getParameterWithKey;
//import static edu.sdsc.queryprocessing.executor.utils.FunctionProcess.translateParameters;
//import static edu.sdsc.queryprocessing.executor.utils.IteratorToIterable.getIterableFromIterator;
//import static edu.sdsc.utils.RelationUtil.insertToTable;
//import static edu.sdsc.utils.RelationUtil.createTable;
//
//// The graphs were stored in the default database of Neo4j in the config file. If this is a operator, then get the graphName by searching ExecutionUtil Table entry, if this is a subopertor, then the name
//// of graph is a global string and should be provided.
//// need to match the subgraph first by tuning the cypher query and then apply page rank on it. The other paramaters are the same with the Tinkerpop one
//// the graph name is the property stored in the default Neo4j databse
//public class ExePageRankNeo4j extends OutputStreamExecutionBase {
//    private List<FunctionParameter> parameters;
//    private boolean limitedNumber=false;
//    private String graphName = null;
//    private Integer numOfNodes=null;
//    private String tableName=null;
//    private JsonObject config;
//    private CypherUtils db_util;
//    private String loc = "defaultGraph";
//    private StreamRelation streamResult;
//
//    // for operators
//    public ExePageRankNeo4j(JsonObject config, FunctionPhysicalOperator ope, VariableTable vte, ExecutionVariableTable evt) {
//        this.config = config;
//        this.db_util = new CypherUtils(config, this.loc);
//        // get the table name from operator's output
//        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        this.tableName = vte.getVarName(ouptVar.first);
//        // set mode
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        // for materialize, only need to get the stream input
//        if (mode.equals(PipelineMode.materializestream)) {
//            // for materialize mode, the only input is the input stream
//            Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//            this.streamResult = ((StreamRelation) evt.getTableEntry(inputVar));
//        }
//        else {
//            JsonArray parameterArray = ope.getParameters();
//            this.parameters = translateParameters(parameterArray);
//            Integer graphID = this.parameters.get(0).getVarID();
//            Neo4JGraph graph = (Neo4JGraph) evt.getTableEntry(new Pair<>(graphID, this.loc)).getValue();
//            this.graphName = graph.getValue();
//            FunctionParameter topk = getParameterWithKey(this.parameters, "topk");
//            if (topk != null) {
//                this.limitedNumber = (boolean) topk.getValueWithExecutionResult(evt);
//                if (this.limitedNumber) {
//                    this.numOfNodes = (Integer) getParameterWithKey(this.parameters, "num").getValueWithExecutionResult(evt);
//                }
//            }
//        }
//    }
//
//    // for sub-operators, provide table name
//    public ExePageRankNeo4j(JsonObject config, String graphName, boolean limitedNumber,  String tableName, Integer... num) {
//        this.config = config;
//        this.db_util = new CypherUtils(config, this.loc);
//        this.tableName = tableName;
//        this.graphName = graphName;
//        this.limitedNumber = limitedNumber;
//        if (this.limitedNumber) {
//            assert num.length == 1;
//            this.numOfNodes = num[0];
//        }
//    }
//
//
//    @Override
//    public ExecutionTableEntryMaterialized execute() {
//        return new MaterializedRelation(getPageRankResult());
////        return materialize();
//    }
//
//    // todo: change the Cypher string
//    @Override
//    public StreamRelation executeStreamOutput() {
////        String cypher = "CALL algo.pageRank.stream(";
////        if (graphName!=null) {
////            String subString = String.format("\'MATCH t WHERE t.graphname=\"%s\"\', ", graphName);
////            cypher += subString;
////        }
////        cypher += "\"MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target\", {graph: \"cypher\"}) YIELD nodeId, score " +
////                "RETURN nodeId as id, score as pagerank";
////        if (limitedNumber) {
////            cypher += String.format(" ORDER BY pagerank DESC LIMIT %d", numOfNodes);
////        }
//        return new StreamRelation(Flowable.fromIterable(getIterableFromIterator(getPageRankResult().iterator())));
//    }
//
////    @Override
////    public ExecutionTableEntryMaterialized materialize(ExecutionTableEntryStream input) {
////        return null;
////    }
//
//    @Override
//    public ExecutionTableEntryMaterialized materialize() {
////        List<String> cols = new ArrayList<>();
////        cols.add("id");
////        cols.add("pagerank");
////        Connection toCon = ParserUtil.sqliteConnect(config);
////        createTable(toCon, tableName, cols);
////
//        // insert to table
////        insertToTable(this.streamResult.getValue(), toCon, tableName, cols);
////        try {
////            toCon.close();
////        } catch (SQLException e) {
////            e.printStackTrace();
////        }
//
////        return new MaterializedRelation(new Pair<>("*", tableName));
////        return new MaterializedRelation(this);
//        return null;
////        er.setValue();
////        return er;
//    }
//
//    private List<AwesomeRecord> getPageRankResult() {
//        String cypher = "CALL algo.pageRank.stream(";
//        if (graphName!=null) {
//            String subString = String.format("\'MATCH t WHERE t.graphname=\"%s\"\', ", graphName);
//            cypher += subString;
//        }
//        cypher += "\"MATCH (n)-->(m) RETURN id(n) AS source, id(m) AS target\", {graph: \"cypher\"}) YIELD nodeId, score " +
//                "RETURN nodeId as id, score as pagerank";
//        if (limitedNumber) {
//            cypher += String.format(" ORDER BY pagerank DESC LIMIT %d", numOfNodes);
//        }
//        return db_util.executeReturn(cypher);
//    }
//
//}
//

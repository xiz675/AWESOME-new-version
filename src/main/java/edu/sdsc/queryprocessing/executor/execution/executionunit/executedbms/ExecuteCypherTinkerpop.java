//package edu.sdsc.queryprocessing.executor.execution.executionunit.executedbms;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.storetosqlite.InMemoryCypherResult;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.datatype.execution.storetosqlite.ResultToStore;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
//import org.apache.tinkerpop.gremlin.structure.Graph;
//import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
//import org.opencypher.gremlin.client.CypherResultSet;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.List;
//import java.util.Optional;
//import java.util.stream.Collectors;
//import java.util.stream.Stream;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//import static edu.sdsc.utils.CypherUtils.getExecutionString;
//import static edu.sdsc.utils.JsonUtil.jsonArrayToStringListWithKey;
//import static edu.sdsc.utils.RelationUtil.createTable;
//import static edu.sdsc.utils.RelationUtil.insertToTable;
//import static edu.sdsc.utils.TinkerpopUtil.*;
//
//// todo: we need to know which variable is graph variable
//public class ExecuteCypherTinkerpop extends OutputStreamExecutionBase {
////  private JsonObject config;
//  private AwesomeGraph graph;
//  private String cypherStatement;
//  private List<String> cols;
//  private String tableName;
////  private Connection toCon;
//
//// if the execution mode is materialize, then need to get the stream result input
//// if the mode is materialize then there is only one input variable and does not need to get the cypher statement
//  public ExecuteCypherTinkerpop(AwesomeGraph graph, ExecuteCypherPhysical ope,  ExecutionVariableTable evt, ExecutionVariableTable... localEvt) {
//    PipelineMode mode = ope.getExecutionMode();
//    this.setExecutionMode(mode);
//    this.graph = graph;
//    this.cols = jsonArrayToStringListWithKey(ope.getSchema(), "name");
//    this.cypherStatement = getExecutionString(ope.getStatement(), ope, false, evt,localEvt);
//  }
//
////  public ExecuteCypherTinkerpop(JsonObject config, ExecuteCypherPhysical ope,  VariableTable vte, ExecutionVariableTable evt, AwesomeGraph graph) {
////    this.graph = graph;
////    this.config = config;
////    this.ope = ope;
////    this.evt = evt;
////    this.vt = vte;
////    this.cypherStatement = getExecutionString(ope.getStatement(), ope, evt);
////    this.cols = jsonArrayToStringListWithKey(ope.getSchema(), "name");
////  }
////
////  public ExecuteCypherTinkerpop(JsonObject config, ExecuteCypherPhysical ope, VariableTable vte, ExecutionVariableTable evt, AwesomeGraph graph, String tableName) {
////    new ExecuteCypherTinkerpop(config, ope, vte, evt, graph);
////    this.tableName = tableName;
////  }
//
////  public ExecutionCypherRecord executeWithBlockingOutput(ExecutionInMemoryGraph inMemoryGraph) {
////    Graph graph = inMemoryGraph.getValue();
////    GraphTraversalSource g = graph.traversal();
////    CypherGremlinClient cypherGremlinClient = CypherGremlinClient.inMemory(g);
////    String cypher = ope.getStatement();
////    cypher = getExecutionString(cypher, ope, evt);
////    return new ExecutionCypherRecord(cypherGremlinClient.submit(cypher));
////  }
//
////    public CypherResultSet executeWithBlockingOutput(ExecutionInMemoryGraph inMemoryGraph) {
////      Graph graph = inMemoryGraph.getValue();
////      GraphTraversalSource g = graph.traversal();
////      CypherGremlinClient cypherGremlinClient = CypherGremlinClient.inMemory(g);
////      String cypher = ope.getStatement();
////      cypher = getExecutionString(cypher, ope, evt);
////      return cypherGremlinClient.submit(cypher);
////  }
//
//  // The input can be set in the constructor and does not have to be here. The result is the ExecutionUtil Relation which is the location
//  // of table that contains the materialized result. For the output variable name, it should be set in the constructors; If that is
//  // a table from an operator, it can be got from the operator, however, if this is from a suboperator, need to pass a name to it.
//
////  public ResultToStore executeWithResult() {
////    CypherResultSet queryResult = executeInMemoryCypher(cypherStatement, graph);
////    return new InMemoryCypherResult(queryResult.all(), cols, tableName);
////  }
////
//
//
//  @Override
//  public MaterializedRelation execute() {
////    Connection toCon = ParserUtil.sqliteConnect(config);
//    System.out.println("executeCypher starts at " + System.currentTimeMillis());
//    CypherResultSet queryResult = executeInMemoryCypher(cypherStatement, graph);
//    List<AwesomeRecord> result = queryResult.stream().map(AwesomeRecord::new).collect(Collectors.toList());
//    System.out.println("executeCypher result size: " + result.size());
//    return new MaterializedRelation(result);
//  }
//
//  @Override
//  public StreamRelation executeStreamOutput() {
//    System.out.println("executeCypher starts at " + System.currentTimeMillis());
//    CypherResultSet queryResult = executeInMemoryCypher(cypherStatement, graph);
//    return new StreamRelation(queryResult.stream().map(AwesomeRecord::new));
//  }
//
//
//  public static void main(String[] args) {
//  }
//}

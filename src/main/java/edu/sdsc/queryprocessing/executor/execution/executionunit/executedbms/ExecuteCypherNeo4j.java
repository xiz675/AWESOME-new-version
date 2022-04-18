//package edu.sdsc.queryprocessing.executor.execution.executionunit.executedbms;
//
//import edu.sdsc.datatype.execution.AwesomeRecord;
//import edu.sdsc.datatype.execution.PipelineMode;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.OutputStreamExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;
//import edu.sdsc.utils.*;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//import io.reactivex.rxjava3.core.Flowable;
//import org.neo4j.dbms.api.DatabaseManagementService;
//import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
//import org.neo4j.graphdb.GraphDatabaseService;
//import org.neo4j.graphdb.Result;
//import org.neo4j.graphdb.Transaction;
//
//import javax.json.JsonObject;
//import java.io.File;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.stream.Collectors;
//
//import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;
//import static edu.sdsc.utils.CypherUtils.getExecutionString;
//import static edu.sdsc.utils.JsonUtil.jsonArrayToStringListWithKey;
//import static edu.sdsc.utils.RelationUtil.createTable;
//import static edu.sdsc.utils.RelationUtil.insertToTable;
//
//public class ExecuteCypherNeo4j extends OutputStreamExecutionBase {
////    private VariableTable vt;
////    private List<String> cols;
////    private String tableName;
//    private StreamRelation streamResult;
//    private String cypherStat;
//    private GraphDatabaseService db;
//
//    // when it is in materialize mode, no need to get the cypher statement
//    public ExecuteCypherNeo4j(GraphDatabaseService db, ExecuteCypherPhysical ope, ExecutionVariableTable evt, boolean optimize, ExecutionVariableTable... localEvt) {
//        PipelineMode mode = ope.getExecutionMode();
//        this.setExecutionMode(mode);
//        this.db = db;
//        this.cypherStat = getExecutionString(ope.getStatement(), ope, optimize, evt, localEvt);
//    }
//
//
//    public MaterializedRelation execute() {
//        System.out.println("executeCypher starts at " + System.currentTimeMillis());
//        List<AwesomeRecord> result = new ArrayList<>();
//        // System.out.println(cypherStat);
//        // todo: if the cypher stat contains "contain" and also the build index is true,
//        //  then should build full text index first, and rewrite the query to call index
//        try (Transaction tx = db.beginTx();
//             Result qr = tx.execute( cypherStat ) )
//        {
//            while ( qr.hasNext() )
//            {
//                result.add(new AwesomeRecord(qr.next()));
//            }
//        }
//        System.out.println(result.size());
//        if (result.size()>0) {
//            System.out.print(result.get(0).toString());
//        }
//        return new MaterializedRelation(result); }
//
//
//    // the Record here is Neo4j.record instead of jooq record
//    public StreamRelation executeStreamOutput() {
//        try (Transaction tx = db.beginTx();
//             Result qr = tx.execute( cypherStat ) )
//        {
//            return new StreamRelation(qr.stream().map(AwesomeRecord::new));
//        }
//    }
//
//
//    public static void main(String[] args) {
//        Path p = Paths.get("C://Users//xw//.Neo4jDesktop//relate-data//dbmss//dbms-5e470448-bc0f-4545-a5aa-81d796164c8c");
//        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(p).build();
////        registerShutdownHook(managementService);
////        managementService.startDatabase("neo4j");
//        GraphDatabaseService db = managementService.database("neo4j");
////        setMaterializedOutput(db);
//        List<AwesomeRecord> result = new ArrayList<>();
//        try (Transaction tx = db.beginTx();
//             Result qr = tx.execute( "MATCH (n) RETURN n.value" ) )
//        {
//            while ( qr.hasNext() )
//            {
//                result.add(new AwesomeRecord(qr.next()));
//            }
//        }
//        System.out.println(result.size());
////
////        graphDb = managementService.database(  );
////        registerShutdownHook( managementService );
////        JsonObject config = LoadConfig.getConfig("newsDB");
////        CypherUtils db_util = new CypherUtils(config, "defaultGraph");
////        String cypherStat = "match (n) return n";
////        List<AwesomeRecord> result = db_util.executeReturn(cypherStat);
////        db_util.close();
//    }
//
//
//}

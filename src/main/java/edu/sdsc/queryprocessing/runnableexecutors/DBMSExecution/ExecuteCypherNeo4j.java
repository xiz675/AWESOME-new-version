package edu.sdsc.queryprocessing.runnableexecutors.DBMSExecution;


import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamOutputRunnable;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import static edu.sdsc.utils.CypherUtils.getExecutionString;


public class ExecuteCypherNeo4j extends AwesomeStreamOutputRunnable<GraphDatabaseService, AwesomeRecord> {
    private String cypherStat;
    private GraphDatabaseService db;

    // when it is in materialize mode, no need to get the cypher statement
    public ExecuteCypherNeo4j(GraphDatabaseService db, ExecuteCypherPhysical ope, ExecutionVariableTable evt, boolean optimize, ExecutionVariableTable... localEvt) {
        super(ope.getExecutionMode());
        this.db = db;
        this.cypherStat = getExecutionString(ope.getStatement(), ope, optimize, evt, localEvt);
    }

    // the Record here is Neo4j.record instead of jooq record
    @Override
    public void executeStreamOutput() {
        try (Transaction tx = db.beginTx();
             Result qr = tx.execute( cypherStat ) )
        {
            setStreamResult(qr.stream().map(AwesomeRecord::new));
        }
    }

    @Override
    public void executeBlocking() {
        System.out.println("executeCypher starts at " + System.currentTimeMillis());
        List<AwesomeRecord> result = new ArrayList<>();
        // System.out.println(cypherStat);
        try (Transaction tx = db.beginTx();
             Result qr = tx.execute( cypherStat ) )
        {
            while ( qr.hasNext() )
            {
                result.add(new AwesomeRecord(qr.next()));
            }
        }
        System.out.println(result.size());
        if (result.size()>0) {
            System.out.print(result.get(0).toString());
        }
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (!this.isStreamIn()) {
            return new MaterializedRelation(this.getMaterializedOutput());
        }
        else {
            return new StreamRelation(this.getStreamResult());
        }
    }

    public static void main(String[] args) {
        Path p = Paths.get("C://Users//xw//.Neo4jDesktop//relate-data//dbmss//dbms-5e470448-bc0f-4545-a5aa-81d796164c8c");
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(p).build();
//        registerShutdownHook(managementService);
//        managementService.startDatabase("neo4j");
        GraphDatabaseService db = managementService.database("neo4j");
//        setMaterializedOutput(db);
        List<AwesomeRecord> result = new ArrayList<>();
        try (Transaction tx = db.beginTx();
             Result qr = tx.execute( "MATCH (n) RETURN n.value" ) )
        {
            while ( qr.hasNext() )
            {
                result.add(new AwesomeRecord(qr.next()));
            }
        }
        System.out.println(result.size());
//
//        graphDb = managementService.database(  );
//        registerShutdownHook( managementService );
//        JsonObject config = LoadConfig.getConfig("newsDB");
//        CypherUtils db_util = new CypherUtils(config, "defaultGraph");
//        String cypherStat = "match (n) return n";
//        List<AwesomeRecord> result = db_util.executeReturn(cypherStat);
//        db_util.close();
    }



}
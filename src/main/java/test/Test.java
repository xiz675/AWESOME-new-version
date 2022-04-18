package test;

import edu.sdsc.utils.LoadConfig;
import edu.sdsc.utils.RDBMSUtils;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statements;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

import javax.json.JsonObject;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static edu.sdsc.queryprocessing.runnableexecutors.graph.ConstructNeo4jGraph.registerShutdownHook;
import static edu.sdsc.utils.SQLParseUtil.getAliasMap;


public class Test {
    private static void testDB() throws SQLException {
        JsonObject db = LoadConfig.getConfig("newsDB");
        RDBMSUtils dbUtils = new RDBMSUtils(db, "News");
        dbUtils.testConnection();
        // should be false
        System.out.println(dbUtils.isTableInDB("test"));
        // should be true
        System.out.println(dbUtils.isTableInDB("t1"));
        // should be 'character'
        System.out.println(dbUtils.getColumnDataType("test", "name"));
        // should be 'integer'

    }

    private static void testParse() throws JSQLParserException, SQLException {
        String sql = "select count(*) as cnt from s";
        Statements stmts = CCJSqlParserUtil.parseStatements(sql);
        List<List<String>> t = new ArrayList<>();
        System.out.println(getAliasMap(sql));
    }

    public static void main(String[] args) {
        Path p = Paths.get("/home/xiz675/neo4j-community-4.2.5");
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder(p).build();
        registerShutdownHook(managementService);
//        managementService.startDatabase("neo4j");
        GraphDatabaseService db = managementService.database("neo4j");
        try (Transaction tx = db.beginTx();
             Result qr = tx.execute( "MATCH (n) RETURN n.value" ) )
        {
            while ( qr.hasNext() )
            {
                System.out.println(qr.next());
            }
        }
    }

}

package edu.sdsc.utils;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.AwesomeGraph;
import io.reactivex.rxjava3.core.Flowable;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.opencypher.gremlin.client.CypherGremlinClient;
import org.opencypher.gremlin.client.CypherResultSet;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static edu.sdsc.utils.RelationUtil.createTable;

public class TinkerpopUtil {
    public static CypherResultSet executeInMemoryCypher(String cypher, Graph graph) {
        GraphTraversalSource g = graph.traversal();
        CypherGremlinClient cypherGremlinClient = CypherGremlinClient.inMemory(g);
        CypherResultSet result = cypherGremlinClient.submit(cypher);
        cypherGremlinClient.close();
        try {
            g.close();
            graph.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }


//    public static void createTable(Connection toCon, String ouptStr, List<String> cols)  {
//        StringBuilder sb = new StringBuilder( 1024 );
//        sb.append( "Create table " ).append( ouptStr ).append( " ( " );
//        for ( int i = 0; i < cols.size(); i ++ ) {
//            if ( i > 0 ) sb.append( ", " );
//            sb.append( cols.get(i) ).append( " " ).append( "text" );
//        }
//        // for columns
//        sb.append( " ) " );
//        try {
//            Statement st = toCon.createStatement();
//            st.execute(sb.toString());
//            st.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }



//    // insert one Tinkerpop result record to the created table
//    public static void materializeStream(String ouptStr, Connection toCon, Publisher<Map<String, Object>> result) {
//
//
//
//
//    }




    public static void storeInMeomoryCypherResult( Iterable<Map<String, Object>> result, List<String> cols, String ouptStr, Connection toCon) throws SQLException {
        Iterator<Map<String, Object>> queryResult = result.iterator();
        createTable(toCon, ouptStr, cols);
        try (PreparedStatement s2 = toCon.prepareStatement(
                "INSERT INTO " + ouptStr + " ("
                        + cols.stream().collect(Collectors.joining(", "))
                        + ") VALUES ("
                        + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
                        + ")"
        )) {
            while (queryResult.hasNext()) {
                Map<String, Object> record = queryResult.next();
                for (int i = 1; i <= cols.size(); i++) {
                    s2.setObject(i, record.get(cols.get(i-1)));
                }
                s2.addBatch();
            }
            s2.executeBatch();
        }

    }


    public static void excuteInMemCypehrStore(List<String> cols, String cypher, String ouptStr, Connection toCon, Graph inMemoryGraph) throws SQLException {
        CypherResultSet queryResult = executeInMemoryCypher(cypher, inMemoryGraph);
//        Graph graph = inMemoryGraph.getValue();
//        GraphTraversalSource g = graph.traversal();
//        CypherGremlinClient cypherGremlinClient = CypherGremlinClient.inMemory(g);
//        Iterator<Map<String, Object>> queryResult = cypherGremlinClient.submit(cypher).iterator();
//        Map<String, Object> firstResult = queryResult.next();
//        List<String> cols = new ArrayList<>(queryResult.next().keySet());

        storeInMeomoryCypherResult(queryResult, cols, ouptStr, toCon);
    }

    public static String getRandomString(int length){
        String str="abcdefghijklmnopqrstuvwxyz";
        Random random=new Random();
        StringBuffer sb=new StringBuffer();
        for(int i=0;i<length;i++){
            int number=random.nextInt(20);
            sb.append(str.charAt(number));
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        for (int i=0; i<10; i++) {
            System.out.println(getRandomString(5));
        }
    }
}

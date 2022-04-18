package edu.sdsc.utils;

import edu.sdsc.datatype.execution.AwesomeRecord;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.neo4j.driver.Records;
import org.reactivestreams.Publisher;

import java.sql.*;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class SQLExecuteUtil {

    public static void excuteStore(String sql, String table, Connection from, Connection to, boolean inMemory) throws SQLException {
        from.setAutoCommit(false);
        PreparedStatement s1 = from.prepareStatement(sql);
        s1.setFetchSize(200);
        try(ResultSet rs = s1.executeQuery()) {
            ResultSetMetaData meta = rs.getMetaData();
            int columnCount = meta.getColumnCount();
            StringBuilder sb = new StringBuilder( 1024 );
            if ( columnCount > 0 ) {
                sb.append( "Create table " ).append( table ).append( " ( " );
            }
            for ( int i = 1; i <= columnCount; i ++ ) {
                if ( i > 1 ) sb.append( ", " );
                String columnName = meta.getColumnLabel( i );
                String columnType = meta.getColumnTypeName( i );

                sb.append( columnName ).append( " " ).append( columnType );

                int precision = meta.getPrecision( i );
                if ( precision != 0 ) {
                    sb.append( "( " ).append( precision ).append( " )" );
                }
            } // for columns
            sb.append( " ) " );
            Statement st = to.createStatement();
            st.execute(sb.toString());
            st.close();
            Long startTime = System.currentTimeMillis();
            List<String> columns = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++){
                columns.add(meta.getColumnLabel(i));
            }

            try (PreparedStatement s2 = to.prepareStatement(
                    "INSERT INTO " + table + " ("
                            + columns.stream().collect(Collectors.joining(", "))
                            + ") VALUES ("
                            + columns.stream().map(c -> "?").collect(Collectors.joining(", "))
                            + ")"
            )) {

                while (rs.next()) {
                    for (int i = 1; i <= meta.getColumnCount(); i++)
                        s2.setObject(i, rs.getObject(i));

                    s2.addBatch();
                }

                s2.executeBatch();
                rs.close();

                Long endTime = System.currentTimeMillis();
                System.out.println("store to sqlite:" + (endTime - startTime) + " ms");
            }
        }
        from.close();
        if (!inMemory) {
            to.close();
        }
    }

//    public static void storeJooqRecords(Publisher<AwesomeRecord> stream, Connection to, String tableName) {
//        boolean hasTable = false;
//        // todo: add schema to relation operator, so there is no need to need to get the first instance to know the cols
//
//
//
//    }
//

    public static Map executeStoreAsMap(Connection from, String table, String col, String key) throws SQLException {
        from.setAutoCommit(false);
        Map<String, String> result = new HashMap<>();
        String sql = String.format("select %s$1, %s$2 from %s$3", col, key, table);
        PreparedStatement s1 = from.prepareStatement(sql);
        s1.setFetchSize(200);
        try(ResultSet rs = s1.executeQuery()) {
            while (rs.next()) {
                String text = rs.getString(col);
                String docId = rs.getString(key);

            }

        }
        return result;
    }


    public static void executeStoreRecords(List<AwesomeRecord> records, Connection toCon, String tbName) {

        

    }



}

package edu.sdsc.utils;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntryMaterialized;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedHLCollection;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import org.reactivestreams.Publisher;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Flow;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RelationUtil {
    public static void groupBy(Connection toCon)  {
        String query = "create table G as (\n" +
                "    select word1, word2, count(*) from GG where word1!='' and word2!='' group by word1, word2\n" +
                ");";
        try {
            Statement st = toCon.createStatement();
            st.execute(query);
            st.close();
            toCon.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }




    public static void createTable(Connection toCon, String ouptStr, List<String> cols)  {
        StringBuilder sb = new StringBuilder( 1024 );
        sb.append( "Create table " ).append( ouptStr ).append( " ( " );
        for ( int i = 0; i < cols.size(); i ++ ) {
            if ( i > 0 ) sb.append( ", " );
            sb.append( cols.get(i) ).append( " " ).append( "text" );
        }
        // for columns
        sb.append( " ) " );
        try {
            Statement st = toCon.createStatement();
            st.execute(sb.toString());
            st.close();
            toCon.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void dropTable(Connection con, String tbName) {
        Statement stmt = null;
        try {
            stmt = con.createStatement();
            String sqlCommand = String.format("DROP TABLE IF EXISTS %s", tbName);
            System.out.println("output : " + stmt.executeUpdate(sqlCommand));
            stmt.close();
            con.commit();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void insertToTable(Publisher<AwesomeRecord> input, Connection toCon, String tableName, List<String> cols) {
        try {
            PreparedStatement s2 = toCon.prepareStatement(
                    "INSERT INTO " + tableName + " ("
                            + cols.stream().collect(Collectors.joining(", "))
                            + ") VALUES ("
                            + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
                            + ")");
            Flowable.fromPublisher(input).forEach(s -> {
                for (int i = 1; i <= cols.size(); i++) {
                    s2.setObject(i, s.getColumn(cols.get(i-1)));
                }
                s2.addBatch();
            });
            s2.executeBatch();
        }
        catch (SQLException e) {
            e.printStackTrace(); }
    }


    // if the index is stored to table as a column, then it can't be a stream
    public static void insertListToTable(List input, Connection toCon, String tableName, List<String> cols, boolean index, boolean isNested) throws SQLException {
        // if the inner elements are lists or needs to add index
        if (isNested || index) {
            List<List<Object>> x = new ArrayList<>();
            int crtIndex = 0;
            for (Object d : input) {
                if (isNested) {
                    List<Object> t = (List<Object>) d;
                    if (index) {
                        t.add(0, crtIndex);
                        crtIndex += 1;
                    }
                    x.add(t);
                }
                else {
                    List<Object> t = new ArrayList<>();
                    t.add(crtIndex);
                    crtIndex+=1;
                    t.add(d);
                    x.add(t);
                }
            }
            insertListToRelationHelper(toCon, x, tableName, cols, true);
        }
        // if the inner elements are primitive and no index needed
        else {
            insertListToRelationHelper(toCon, input, tableName, cols, false);
        }
    }


    // todo: needs to know if this is a HL and needs to know if this is a nested list
    public static void insertStreamToTable(Stream data, Connection toCon, String tableName, List<String> cols, boolean index, boolean isHL) throws SQLException {
        // each element has to be materialized
        Optional first = data.findFirst();
        if (first.isEmpty()) {
            return ;
        }
        Object firstValue = first.get();
        // if index, has to materialize and has to be a list
        if (index) {
            // if HL, needs to get value first
            List<List<Object>> values = new ArrayList<>();
            if (isHL) {
                // if nested
                if (firstValue instanceof MaterializedList) {
                    Stream<List<Object>> x = data.map(i -> ((MaterializedList) i).getValue());
                    values = x.collect(Collectors.toList());
                }
                // if not nested, needs to make it a list
                else {
                    Stream<Object> x = data.map(i -> ((ExecutionTableEntryMaterialized) i).getValue());
                    List<Object> tempList = x.collect(Collectors.toList());
                    for (Object d : tempList) {
                        List<Object> t = new ArrayList<>();
                        t.add(d);
                        values.add(t);
                    }
                }
            }
            // if it is not HL, do not need to get value and directly add index and insert
            else {
                if (firstValue instanceof List) {
                    Stream<List<Object>> x = data.map(i -> (List<Object>) i);
                    values = x.collect(Collectors.toList());
                }
                // if not nested, need to make it a list
                else {
                    Stream<Object> x = data.map(i -> (Object) i);
                    List<Object> tempList = x.collect(Collectors.toList());
                    for (Object d : tempList) {
                        List<Object> t = new ArrayList<>();
                        t.add(d);
                        values.add(t);
                    }
                }
            }
            int crtIndex = 0;
            for (List<Object> v : values) {
                v.add(0, crtIndex);
                crtIndex += 1;
            }
            insertListToRelationHelper(toCon, values, tableName, cols, true);
        }
        // if no index, do not need to materialize
        else {
            if (isHL) {
                // if HL, get value and insert list, no need to materialize
                if (firstValue instanceof MaterializedList) {
                    insertStreamToRelationHelper(toCon, data, tableName, cols, true, true);
                }
                // if not nested, get value and insert object
                else {
                    insertStreamToRelationHelper(toCon, data, tableName, cols, true, false);
                }
            }
            else {
                if (firstValue instanceof List) {
                    insertStreamToRelationHelper(toCon, data, tableName, cols, false, true);
                }
                else {
                    insertStreamToRelationHelper(toCon, data, tableName, cols, false, true);
                }
            }
        }
    }


    private static void insertListToRelationHelper(Connection toCon, List inputList, String tableName, List<String> colNames, boolean isList) {
        try (PreparedStatement s2 = toCon.prepareStatement(
                "INSERT INTO " + tableName + " ("
                        + colNames.stream().collect(Collectors.joining(", "))
                        + ") VALUES ("
                        + colNames.stream().map(c -> "?").collect(Collectors.joining(", "))
                        + ")"))
        {
            if (isList) {
                for (Object d : inputList) {
                    List<Object> t = (List<Object>) d;
                    for (int i = 1; i <= colNames.size(); i++) {
                        s2.setObject(i, t.get(i-1));
                    }
                    s2.addBatch();
                }
                s2.executeBatch(); }
            else {
                assert colNames.size() == 1;
                for (Object d : inputList) {
                    s2.setObject(1, d);
                    s2.addBatch();
                }
                s2.executeBatch();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    private static void insertStreamToRelationHelper(Connection toCon, Stream input, String tableName, List<String> colNames, boolean isHL, boolean isList) {
        try {
            PreparedStatement s2 = toCon.prepareStatement(
                    "INSERT INTO " + tableName + " ("
                            + colNames.stream().collect(Collectors.joining(", "))
                            + ") VALUES ("
                            + colNames.stream().map(c -> "?").collect(Collectors.joining(", "))
                            + ")");
            if (isHL) {
                if (isList) {
                    input.forEach(d -> {
                        List<Object> l = (List<Object>) ((MaterializedList) d).getValue();
                        try {
                            for (int i = 1; i <= colNames.size(); i++) {
                                s2.setObject(i, l.get(i-1));
                            }
                            s2.addBatch();
                        }
                        catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    });
                    s2.executeBatch();
                }
                else {
                    assert colNames.size() == 1;
                    input.forEach(d -> {
                        Object t = ((ExecutionTableEntryMaterialized) d).getValue();
                        try {
                            s2.setObject(1, t);
                            s2.addBatch();
                        }
                        catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    });
                    s2.executeBatch();
                }
            }
            else {
                if (isList) {
                    input.forEach(d -> {
                        List<Object> l = (List<Object>) d;
                        try {
                            for (int i = 1; i <= colNames.size(); i++) {
                                s2.setObject(i, l.get(i-1));
                            }
                            s2.addBatch();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    });
                    s2.executeBatch();
                }
                else {
                    input.forEach(d -> {
                        try {
                            s2.setObject(1, d);
                            s2.addBatch();
                        } catch (SQLException throwables) {
                            throwables.printStackTrace();
                        }
                    });
                    s2.executeBatch();
                }
            }
        }
        catch (SQLException e) {
            e.printStackTrace(); }
    }


    public static void insertAwesomRecordToTable(Iterable<AwesomeRecord> input, Connection toCon, String tableName, List<String> cols) throws SQLException {
        try (PreparedStatement s2 = toCon.prepareStatement(
                    "INSERT INTO " + tableName + " ("
                            + cols.stream().collect(Collectors.joining(", "))
                            + ") VALUES ("
                            + cols.stream().map(c -> "?").collect(Collectors.joining(", "))
                            + ")"))
        {
            for (AwesomeRecord s : input) {
                for (int i = 1; i <= cols.size(); i++) {
                    s2.setObject(i, s.getColumn(cols.get(i-1)));
                }
                s2.addBatch();
            }
            s2.executeBatch();
        }
    }

}

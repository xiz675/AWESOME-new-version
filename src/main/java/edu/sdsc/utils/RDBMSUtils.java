package edu.sdsc.utils;

import javax.json.JsonObject;
import java.sql.*;
import java.util.*;

public class RDBMSUtils {
    // your database name
    // this implementation assumes you have only one database
    // if you want to support multi-database, you need to maintain a collection of db connections

    private Connection connection = null;
    private static final String GET_EST_TBL_SIZE = "SELECT reltuples AS estimate FROM pg_class where relname = '%s'";
    private static final String GET_ALL_TBL_SQL = "select tablename from pg_tables where schemaname='public'";
    //  this is a cache to hole all table names
    private Set<String> tableNameSet = null;
    private static final String GET_COL_TYPE_SQL = "select column_name, data_type " +
            "from information_schema.columns " +
            "where table_schema='public' and table_name='%s'";
    private Map<String, Map<String, String>> columnTypeMap = new HashMap<>();

    // initialize db connection
    public RDBMSUtils(JsonObject config, String dbname) {
        // if it is a SQLite relation
        if (dbname.equals("*")) {
            connection = ParserUtil.sqliteConnect(config);
        }
        else {
            JsonObject dbConfig = config.getJsonObject(dbname);
            Properties prop = new Properties();
            prop.setProperty("user", dbConfig.getString("userName"));
            prop.setProperty("password", dbConfig.getString("password"));
            try {
                connection = DriverManager.getConnection(String.format(dbConfig.getString("URL"), dbConfig.getString("dbName")), prop);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public Connection getConnection() {
        return connection;
    }

    public void closeConnection() {
        try {
            connection.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    // util function to test whether the db is connected properly
    public void testConnection() {
        if (connection != null) {
            System.out.println("Connection Success!");
        }
    }

    // retrieve all table names from a database
    public void updateTableNameSet() throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(GET_ALL_TBL_SQL);
        tableNameSet = new HashSet<>();
        while (rs.next()) {
            tableNameSet.add(rs.getString(1));
        }
        rs.close();
        st.close();
    }

    public void deleteTable(String tableName) {
        String sql = String.format("DROP TABLE %s", tableName);
        try {
            Statement st = connection.createStatement();
            st.executeUpdate(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(String.format("Table %s deleted in given database", tableName));
    }

    public boolean isTableInDB(String tableName)  {
        try {
            if (tableNameSet == null) {
                updateTableNameSet();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        assert tableNameSet != null;
        return tableNameSet.contains(tableName.toLowerCase());
    }

    public Set<String> getTableNameSet() {
        return tableNameSet;
    }


    public void updateColumnTypeMap(String tableName) throws SQLException {
        assert isTableInDB(tableName);

        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(String.format(GET_COL_TYPE_SQL, tableName));
        Map<String, String> typeMap = new HashMap<>();
        while (rs.next()) {
            typeMap.put(rs.getString(1), rs.getString(2));
        }
        columnTypeMap.put(tableName, typeMap);
        rs.close();
        st.close();
    }

    public ResultSet getResult(String sql) throws SQLException {
        Statement st = connection.createStatement();
        ResultSet rs = st.executeQuery(sql);
        return rs;
    }

    public String getColumnDataType(String tableName, String columnName) {
        Map<String, String> typeMap = getColumnTypeMap(tableName);
        return typeMap.get(columnName);
    }
    public Map<String, String> getColumnTypeMap(String tableName) {
        try {
            if (columnTypeMap.get(tableName) == null) {
                updateColumnTypeMap(tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
            System.exit(1);
        }
        assert columnTypeMap.get(tableName) != null;
        return columnTypeMap.get(tableName);
    }

    public static Integer getTableSize(JsonObject config, String dbName, String tableName) throws SQLException {
        RDBMSUtils db_util = new RDBMSUtils(config, dbName);
        Connection con = db_util.getConnection();
        Statement st = con.createStatement();
        ResultSet rs = st.executeQuery(String.format(GET_EST_TBL_SIZE, tableName));
        rs.next();
        Integer result = rs.getInt(1);
        rs.close();
        st.close();
        return result;
    }


}

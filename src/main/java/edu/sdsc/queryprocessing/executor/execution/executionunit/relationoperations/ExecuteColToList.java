//package edu.sdsc.queryprocessing.executor.execution.executionunit.relationoperations;
//
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.planer.physicalplan.element.ColumnToList;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//
//import javax.json.JsonObject;
//import java.sql.*;
//import java.util.ArrayList;
//
//public class ExecuteColToList {
//    private String rName;
//    private String cName;
//    private Connection con;
//    private ColumnToList ope;
//    private ExecutionVariableTable evt;
//    private Statement stmt;
//
//    public ExecuteColToList(JsonObject config, ColumnToList col,  ExecutionVariableTable evt) {
//          this.cName = col.getColName();
//          this.rName = col.getrName();
//          this.con = ParserUtil.sqliteConnect(config);
//          this.ope = col;
//          this.evt = evt;
//    }
//
//    public void execute() {
//        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        MaterializedList ext;
//        ArrayList<Object> rsult = null;
//        try {
//            rsult = getColumnFromTable();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        if (rsult.get(0) instanceof Integer) {
//            ArrayList<Integer> intRsult = new ArrayList<>();
//            for (Object i : rsult) {
//                intRsult.add((Integer) i);
//            }
//            ext = new MaterializedList<>(intRsult);
//        }
//        else {
//            ArrayList<String> intRsult = new ArrayList<>();
//            for (Object i : rsult) {
//                intRsult.add((String) i);
//            }
//            ext = new MaterializedList<>(intRsult);
//        }
//        try {
//            con.close();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        evt.insertEntry(ouptVar, ext);
//    }
//
//
//    private ArrayList<Object> getColumnFromTable() throws SQLException {
//        ArrayList<Object> result = new ArrayList<>();
//        if (stmt == null) {
//            stmt = con.createStatement();
//        }
//
//        String sql = String.format("select %s from %s", cName, rName);
//        ResultSet resultSet = stmt.executeQuery(sql);
//        ResultSetMetaData metaData = resultSet.getMetaData();
//
//        if (metaData.getColumnCount() != 1) {
//            throw new RuntimeException("Only one column is allowed.");
//        }
//
//        while (resultSet.next()) {
//            if (metaData.getColumnTypeName(1).equals("INTEGER")) {
//                result.add(resultSet.getInt(cName));
//            } else {
//                result.add(resultSet.getString(cName));
//            }
//        }
//        return result;
//    }
//}

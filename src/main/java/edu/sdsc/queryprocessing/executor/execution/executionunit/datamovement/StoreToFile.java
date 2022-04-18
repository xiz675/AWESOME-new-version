//package edu.sdsc.queryprocessing.executor.execution.ExecutionUnit.datamovement;
//
//import edu.sdsc.queryprocessing.executor.execution.ElementVariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.executor.execution.ExecutionUnit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.planer.physicalplan.element.SQLiteToFile;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//
//import javax.json.JsonObject;
//import java.io.BufferedWriter;
//import java.io.FileWriter;
//import java.io.Writer;
//import java.sql.Connection;
//import java.sql.ResultSet;
//import java.sql.Statement;
//
//public class StoreToFile implements BlockExecutionBase {
//    private SQLiteToFile op;
//    private ExecutionVariableTable evt;
//    private VariableTable vt;
//    private JsonObject config;
//    public StoreToFile(JsonObject config, SQLiteToFile op, ExecutionVariableTable evt) {
//        this.config = config;
//        this.op = op;
//        this.evt = evt;
//
//    }
//    @Override
//    public void execute() {
//        Connection con = ParserUtil.sqliteConnect(config);
//        try {
//            Statement st = con.createStatement();
//            String sql = op.getSql();
//            String dataPath = op.getPath();
//            ResultSet rs = st.executeQuery(sql);
//            Writer output;
//            output = new BufferedWriter(new FileWriter(dataPath));
//            while (rs.next()) {
//                String word = rs.getString(1);
//                output.write(word + System.getProperty("line.separator"));
//            }
//            output.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//
//    private void writeTotxt() {
//
//    }
//
//}

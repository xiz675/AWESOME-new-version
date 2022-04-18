//package edu.sdsc.queryprocessing.executor.execution.executionunit.functions;
//
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionVariableTable;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.UnionPhysical;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//
//import javax.json.JsonArray;
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//import java.sql.Statement;
//
//public class ExecuteUnion {
//    private UnionPhysical ope;
//    private ExecutionVariableTable evt;
//    private VariableTable vt;
//    private JsonObject config;
//    public ExecuteUnion(JsonObject config, UnionPhysical ope, VariableTable vte, ExecutionVariableTable ev) {
//        this.config = config;
//        this.ope = ope;
//        this.vt = vte;
//        this.evt = ev;
//
//    }
//    public void execute() throws SQLException {
//        JsonArray parameter = ope.getParameters();
//        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        String ouptStr = vt.getVarName(ouptVar.first);
//        String col1 = parameter.getJsonObject(0).getString("varName");
//        String col2 = parameter.getJsonObject(1).getString("varName");
//        String colName = parameter.getJsonObject(2).getString("value");
//        Connection con = ParserUtil.sqliteConnect(config);
//        Statement st = con.createStatement();
//        st.setFetchSize(100);
//        String[] x1 = col1.split("\\.");
//        String[] x2 = col2.split("\\.");
//        String sql = "CREATE TABLE " + ouptStr + " AS SELECT " + x1[1] + " as " + colName + " from " + x1[0] +
//                " union select " + x2[1] + " from " + x2[0];
//        System.out.println(sql);
//        st.execute(sql);
//        //ResultSet rs = st.executeQuery("select " + x[1] + " from " + x[0]);
//        st.close();
//        MaterializedRelation er = new MaterializedRelation(new Pair<>("*", ouptStr) );
////        er.setValue(new Pair<String, String>("*", ouptStr));
//        evt.insertEntry(ouptVar, er);
//    }
//}

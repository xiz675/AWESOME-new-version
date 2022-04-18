//package edu.sdsc.queryprocessing.executor.execution.executionunit.datamovement;
//
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.Materialize;
//import edu.sdsc.utils.RDBMSUtils;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.utils.SQLExecuteUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.sql.SQLException;
//
//// materialize to postgres
//public class ExecuteMaterialize {
//    private RDBMSUtils db_util;
//    private Materialize ope;
//    private ExecutionVariableTable evt;
//    private VariableTable vt;
//    private JsonObject config;
//
//    public ExecuteMaterialize(JsonObject config, Materialize ope, VariableTable vte, ExecutionVariableTable evt) {
//        String loc = ope.getTrgtMachine();
//        this.config = config;
//        this.db_util = new RDBMSUtils(config, loc);
//        this.ope = ope;
//        this.evt = evt;
//        this.vt = vte;
//    }
//
//    public void execute() throws SQLException {
//        Connection toCon = db_util.getConnection();
//        Connection fromCon = ParserUtil.sqliteConnect(config);
//        Pair<Integer, String> inputVar = ope.getInputVar().iterator().next();
//        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        String inputStr = vt.getVarName(inputVar.first);
//        String ouptStr = vt.getVarName(ouptVar.first);
//        String sql = "select * from " + inputStr;
//        SQLExecuteUtil.excuteStore(sql, ouptStr, fromCon, toCon, true);
//        toCon.close();
//        fromCon.close();
//        MaterializedRelation er = new MaterializedRelation(new Pair<String, String>(ope.getTrgtMachine(), ouptStr));
//        evt.insertEntry(ouptVar, er);
//    }
//}

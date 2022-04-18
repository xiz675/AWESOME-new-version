//package edu.sdsc.queryprocessing.executor.execution.executionunit.textoperations;
//
//import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelationName;
//import edu.sdsc.queryprocessing.executor.execution.executionunit.BlockExecutionBase;
//import edu.sdsc.queryprocessing.planner.physicalplan.element.functionoperators.AutoPhrasePhysical;
//import edu.sdsc.utils.AutoPhraseUtil;
//import edu.sdsc.utils.Pair;
//import edu.sdsc.utils.ParserUtil;
//import edu.sdsc.variables.logicalvariables.VariableTable;
//
//import javax.json.JsonArray;
//import javax.json.JsonObject;
//import java.sql.Connection;
//import java.util.List;
//
//public class ExecuteAutoPhrase extends BlockExecutionBase {
//    private AutoPhrasePhysical ope;
//    private VariableTable vt;
//    private List<String> testCorpus;
//    private JsonObject config;
//
//    public ExecuteAutoPhrase(JsonObject config, AutoPhrasePhysical ope, VariableTable vte) {
//        this.config = config;
//        this.ope = ope;
//        this.vt = vte;
//    }
//
//    public MaterializedRelationName execute()  {
//        Pair<Integer, String> ouptVar = ope.getOutputVar().iterator().next();
//        JsonArray parameter = ope.getParameters();
//        String ouptStr = vt.getVarName(ouptVar.first);
//        Connection con = ParserUtil.sqliteConnect(config);
//        String col = parameter.getJsonObject(0).getString("varName");
//        Integer min_sup = parameter.getJsonObject(1).getInt("value");
//        Integer num = parameter.getJsonObject(2).getInt("value");
//        AutoPhraseUtil autoPhrase = new AutoPhraseUtil(con, ouptStr, col, min_sup, num);
//        autoPhrase.callScript();
//        return new MaterializedRelationName(new Pair<>("*", ouptStr));
////        er.setValue();
////        evt.insertEntry(ouptVar, er);
//    }
//}

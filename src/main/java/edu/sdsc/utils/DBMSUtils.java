package edu.sdsc.utils;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.DBPhysical;

import java.util.HashMap;
import java.util.Map;

import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.getTableEntryWithLocal;

public class DBMSUtils {

  public static String generateStatement(DBPhysical ope, String sql, ExecutionVariableTable evt, boolean ifSQL, ExecutionVariableTable... localEvt) {
    Map<Integer, String> varStr = new HashMap<>();
    Map<Integer, Pair<Integer, Integer>> temp = ope.getVarPosition();
    if (temp != null) {
      for (int i : temp.keySet()) {
        varStr.put(i, sql.substring(temp.get(i).first, temp.get(i).second));
      }
    }
//        Integer tempVar;
//        String tempvarStr;
    for (Integer s : temp.keySet()) {
      Pair<Integer, String> tempVarKey = new Pair<>(s, "*");
      if ((localEvt.length>0 && localEvt[0].hasTableEntry(tempVarKey)) || evt.hasTableEntry(tempVarKey)) {
        ExecutionTableEntry value = getTableEntryWithLocal(tempVarKey, evt, localEvt);
        if (value instanceof AwesomeInteger || value instanceof AwesomeString || value instanceof MaterializedList) {
          String realVal;
          if (ifSQL) {
            realVal = value.toSQL();
          }
          else {
            realVal = ((ExecutionTableEntryMaterialized<?>) value).toSolr();
          }
          sql = sql.replaceAll("\\$" + varStr.get(s), realVal);
          if (value instanceof MaterializedList) {
            System.out.println("keywords list size: " + ((MaterializedList) value).getValue().size() );
          }
        }
      }
    }

    sql = sql.replaceAll("\\$", "");
    return sql;
  }
}

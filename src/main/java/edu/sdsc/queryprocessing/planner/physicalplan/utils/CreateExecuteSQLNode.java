package edu.sdsc.queryprocessing.planner.physicalplan.utils;

import edu.sdsc.queryprocessing.planner.physicalplan.element.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ExecuteSQL;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.ExecuteSQLPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.LoadTable;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.Store2SQLite;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonObject;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static edu.sdsc.utils.RDBMSUtils.getTableSize;

// todo: when the sql query use tables from both postgres and Awesome, needs to decide here where should join happen. In the future, maybe need to split the query
//  to different parts on different tables, then consider an in-memory join
public class CreateExecuteSQLNode {
    //Map<String, String> tableToURL = new HashMap<>();
    Map<Integer, List<String>> variableToUnit;
    HashSet<String> loadToMemoryTables = new HashSet<>();
    VariableTable vt;
    final Integer threshold = 5_000;
    // variableToUnit stores the execution units of in-memory tables. A table used may already be materialized in this execution unit
    // in this case, do not need to materialize it again
    public CreateExecuteSQLNode(VariableTable vt) {
        this.variableToUnit = new HashMap<>();
        this.vt = vt;
    }
    public static PhysicalOperator getStore2SQLiteNode(String t, Integer varID, Integer outVarID) {
        Store2SQLite temp = new Store2SQLite(t);
        Set<Pair<Integer, String>> input = new HashSet<>();
        Set<Pair<Integer, String>> output = new HashSet<>();
        input.add(new Pair<>(varID, "*"));
        output.add(new Pair<>(outVarID, "*"));
        temp.setInputVar(input);
        temp.setOutputVar(output);
        return temp;
    }
    private ExecuteSQLPhysical getExecuteSQLOperator(ExecuteSQL x, Set<Pair<Integer, String>> inputVar){
        String executeLoc = x.getDbName();
        ExecuteSQLPhysical executeSQL = new ExecuteSQLPhysical(x);
        executeSQL.setExecuteUnit(executeLoc);
        executeSQL.setInputVar(inputVar);
        return executeSQL;
    }

    // get nodes for passed variables, this part does not include the executeSQL physical operator
    private List<PhysicalOperator> getCommonNodes(ExecuteSQL x, JsonObject config, Set<Pair<Integer, String>> inputVar, Integer id, boolean optimize, boolean suboperator)  {
//        List<String> tables = x.getTables();
//        Collections.sort(tables);
//        boolean toMaterialize = false;
//        boolean toExtract = false;
        // todo: when parsing, for awesome table, needs to decide the varID and varName (table name ).
        List<String> tables = x.getTables();
        List<String> AwesomeTables = new ArrayList<>();
        List<String> DBTables = new ArrayList<>();
        for (String t : tables) {
            if (!t.startsWith("$")) {
                DBTables.add(t);
            }
            else {
                AwesomeTables.add(t);
            }
        }
        List<PhysicalOperator> result = new ArrayList<>();
        Set<Integer> varIDs = x.getInputVar();
        String executeLoc = x.getDbName();

        boolean executeInDB = true;
        // may need to change actual location when there are awesome tables
        if (AwesomeTables.size() != 0) {
//            boolean underThres = true;
            // if optimize, then execute in sqlite in memory


            // else execute in postgres

            // if all DB tables sizes are under a threshold
//            for (String t : DBTables) {
//                try {
//                    if (getTableSize(config, executeLoc, t) > this.threshold) {
//                        underThres = false;
//                        break;
//                    }
//                } catch (SQLException throwables) {
//                    throwables.printStackTrace();
//                }
//            }
            if (optimize) {
                executeInDB = false;
                executeLoc = "*";
            }
        }

        // add inputVar for executeSQL physical operator
        for (Integer y : varIDs) {
            // if this is not a relation, do not need to materialize
            if (!vt.getVarType(y).equals("Relation") ) {
                inputVar.add(new Pair<>(y, "*"));
            }
            // if this is a relation, needs to decide whether to materialize it first and change the input variable location
            else if (!suboperator) {
                // materialize the in-memory tables
                if (executeInDB && (variableToUnit.get(y) == null || !variableToUnit.get(y).contains(executeLoc))) {
                    List<String> tempList = variableToUnit.get(y);
                    if (tempList!= null && tempList.contains(executeLoc)) {
                        continue;
                    }
                    if (tempList == null) {
                        tempList = new ArrayList<>();
                    }
                    tempList.add(executeLoc);
                    variableToUnit.put(y, tempList);
                    inputVar.add(new Pair<>(y, executeLoc));
                    result.add(getMaterializedNode(vt.getVarName(y), y, executeLoc));
                }
                else if (!executeInDB) {
                    result.add(getStore2SQLiteNode(vt.getVarName(y), y, id));
                    inputVar.add(new Pair<>(id, executeLoc));
                    id = id-1;
                }
            }
        }
        // if executeInMemory, then need to extract tables from DB, and add input variables to executeSQL,
        //  also need to add store2SQLite nodes for Awesome tables.
        if (!executeInDB) {
            for (String t : DBTables) {
                // if the table is already loaded before, then do not need to load again
                if (!loadToMemoryTables.contains(t)) {
                    result.add(getLoadTableNode(x.getDbName(), t, id));
                    inputVar.add(new Pair<>(id, "*"));
                    id = id - 1;
                }
            }
        }

        x.setDbName(executeLoc);
        return result;
    }


    // the physical plan for without pipeline version should not be different from the pipeline version.
    // The only difference should be that the execution mode is different
    public List<PhysicalOperator> getNodes(ExecuteSQL x, JsonObject config, boolean unified, Integer intermediateVarID, boolean optimized, boolean suboperator)  {
        Set<Pair<Integer, String>> inputVar = new HashSet<>();
        List<PhysicalOperator> tempResults = new ArrayList<>(getCommonNodes(x, config, inputVar, intermediateVarID, optimized, suboperator));
        Set<Pair<Integer, String>> outputVar = new HashSet<>();
        Set<Integer> outputID = x.getOutputVar();
        ExecuteSQLPhysical executeSQL = getExecuteSQLOperator(x, inputVar);
        // todo : get new intermediate var ID if there are loadtable or store2sqlite nodes
        Integer newIntermediate = intermediateVarID;
        if (tempResults.size()>0) {
            for (int i = tempResults.size()-1; i>=0; i-- ) {
                Set<Pair<Integer, String>> out = tempResults.get(i).getOutputVar();
                Integer temp = out.iterator().next().first;
                if (temp <= intermediateVarID) {
                    newIntermediate = temp-1;
                    break;
                }
            }
        }
        String executeLoc = x.getDbName();
        executeSQL.setIntermediateVarID(newIntermediate);
        if (!unified) {
            for (Integer id : outputID) {
                outputVar.add(new Pair<>(id, executeLoc));
            }
            executeSQL.setOutputVar(outputVar);
            tempResults.add(executeSQL);
            PostgresToSQLite toMemory = new PostgresToSQLite(outputVar);
            tempResults.add(toMemory);
        }
        else {
            for (Integer id : outputID) {
                outputVar.add(new Pair<>(id, "*"));
            }
//            ExecuteSQLPhysical executeSQL = getExecuteSQLOperator(x, inputVar);
            executeSQL.setOutputVar(outputVar);
            tempResults.add(executeSQL);
        }
        return tempResults;
    }


    public static PhysicalOperator getMaterializedNode(String tbName, Integer y, String executeLoc) {
        Materialize tempNode = new Materialize(tbName, y, executeLoc);
        Set<Pair<Integer, String>> input = new HashSet<>();
        Set<Pair<Integer, String>> output = new HashSet<>();
        input.add(new Pair<>(y, "*"));
        output.add(new Pair<>(y, executeLoc));
        tempNode.setInputVar(input);
        tempNode.setOutputVar(output);
        return tempNode;
    }

    private PhysicalOperator getLoadTableNode(String dbName, String tableName, Integer varID) {
        LoadTable temp = new LoadTable(dbName, tableName);
        Set<Pair<Integer, String>> output = new HashSet<>();
        output.add(new Pair<>(varID, "*"));
        temp.setOutputVar(output);
        loadToMemoryTables.add(tableName);
        return temp;
    }




//    public List<PhysicalOperator> getNodes(ExecuteSQL x)  {
//        Set<Pair<Integer, String>> inputVar = new HashSet<>();
//        List<PhysicalOperator> tempResults = getCommonNodes(x, inputVar);
//        Set<Pair<Integer, String>> outputVar = new HashSet<>();
//        Set<Integer> outputID = x.getOutputVar();
//        for (Integer id : outputID) {
//            outputVar.add(new Pair<>(id, "*"));
//        }
//        ExecuteSQLPhysical executeSQL = getExecuteSQLOperator(x, inputVar);
//        executeSQL.setOutputVar(outputVar);
//        tempResults.add(executeSQL);
//        return tempResults;
//    }

}

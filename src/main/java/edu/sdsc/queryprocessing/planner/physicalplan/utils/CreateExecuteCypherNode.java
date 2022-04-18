package edu.sdsc.queryprocessing.planner.physicalplan.utils;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.ExecuteCypher;
import edu.sdsc.queryprocessing.planner.physicalplan.element.*;

import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.ExecuteCypherPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.Neo4jToSQLite;
import edu.sdsc.queryprocessing.planner.physicalplan.element.relationoperators.GetColumnPhysical;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.neo4j.graphdb.GraphDatabaseService;

import java.util.*;

public class CreateExecuteCypherNode {
    Map<Pair<Integer, String>, Integer> indexMap;
    Map<String, GraphDatabaseService> connectedDB = new HashMap<>();
    Integer newVariableIdx;
    VariableTable vt;
    public CreateExecuteCypherNode(VariableTable vt) {
        this.indexMap = new HashMap<>();
        this.vt = vt;
        this.newVariableIdx = vt.getSize()+1;

    }

    // for SQL or Cypher inside a HLO, need to change the outputOperator to the last one's
    public List<PhysicalOperator> getNodes(ExecuteCypher x, boolean unify)  {
        Set<Integer> varIDs = x.getInputVar();
        List<PhysicalOperator> result = new ArrayList<>();
        Set<Pair<Integer, String>> inputVar = new HashSet<>();
        Set<Pair<Integer, String>> outputVar = new HashSet<>();
        Set<Integer> outputID = x.getOutputVar();
        String executeLoc = x.getDbName();
        String stat = x.getStatement();
        Map<Integer, Pair<Integer, Integer>> varPos = x.getVarPosition();
        if (x.getGraphID() != null) {
            inputVar.add(new Pair<>(x.getGraphID(), "*"));
            varIDs.remove(x.getGraphID());
        }
        // process variables passed to Cypher query, but the graph input variable is not required
        for (Integer y : varIDs) {
            if (!vt.getVarType(y).equals("Relation")) {
                // assume that variables passed to it must be a in memory variables
                inputVar.add(new Pair<>(y, "*"));
            }
            else {
                // inputVar.add(new Pair<>(y, "*"));
                Integer start = varPos.get(y).first;
                Integer end = varPos.get(y).second;
                String varName = stat.substring( start, end);
                Pair<Integer, String> varPair;

                if (varName.contains(".")) {
                    String cName = varName.split("\\.")[1];
                    varPair = new Pair<>(y, cName);
                }
                else {
                    varPair = new Pair<>(y, "");
                }
                if (!indexMap.containsKey(varPair)) {
                    PhysicalOperator tempNode;
                    Set<Pair<Integer, String>> input = new HashSet<>();
                    Set<Pair<Integer, String>> output = new HashSet<>();
                    input.add(new Pair<>(y, "*"));
                    Pair<Integer, String> newVar = new Pair<>(newVariableIdx, "*");
                    indexMap.put(varPair, newVariableIdx);
                    varPos.remove(y);
                    varPos.put(newVariableIdx, new Pair<>(start, end));
                    output.add(newVar);
                    newVariableIdx += 1;
                    if (varName.contains(".")) {
                        tempNode = new GetColumnPhysical(y,varName.split("\\.")[0], varPair.second );
//                        tempNode = new ColumnToList(varName.split("\\.")[0], varPair.second);
                    }
                    else {
                        tempNode = new RelationToList();
                    }
                    tempNode.setInputVar(input);
                    tempNode.setOutputVar(output);
                    result.add(tempNode);
                    inputVar.add(newVar);
                }
                else {
                    int tempIdx = indexMap.get(varPair);
                    inputVar.add(new Pair<>(tempIdx, "*"));
                }

            }
        }
        ExecuteCypherPhysical executeC = new ExecuteCypherPhysical(x, connectedDB);
        executeC.setExecuteUnit(executeLoc);
        executeC.setInputVar(inputVar);
        executeC.setVarPosition(varPos);
        if (!unify) {
            for (Integer id : outputID) {
                outputVar.add(new Pair<>(id, executeLoc));
            }
            executeC.setOutputVar(outputVar);
            result.add(executeC);
            // only when without pipeline, the result is stored in SQLite, it only affects execution mode and does not need to change
            // the plan
            Neo4jToSQLite toMemory = new Neo4jToSQLite(outputVar);
            result.add(toMemory);
        }
        else {
            for (Integer id : outputID) {
                outputVar.add(new Pair<>(id, "*"));
            }
            // when with pipeline, there is no need to store it to SQLite unless it is in blocking mode
            executeC.setOutputVar(outputVar);
            result.add(executeC);
        }
        return result;
    }
}

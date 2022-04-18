package edu.sdsc.queryprocessing.planner.logicalplan.utils;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Functions.*;
import edu.sdsc.utils.JsonUtil;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;
import java.util.Map;

import static edu.sdsc.queryprocessing.planner.logicalplan.utils.SQLUtil.handleVar;
import static edu.sdsc.queryprocessing.planner.logicalplan.utils.SQLUtil.processSQLTemplate;

public class CreateNodes {


    enum funcNames {
        arrayAggregate, createHistogram, constructGraphFromDoc, NER, pageRank,
        countMatrix, autoPhrase, union, tdm, topicModel, tokenize, lda, add, sum,
        getValue, rowNames, buildWordNeighborGraph, stringJoin, stringFlat, stringReplace, toList

    }

    enum DBTypes {
        SQLQuery, AwsmSQL,Neo4jQuery, AwsmCypher, Solr
    }

//    public static ExecuteSolr createSolrNode(JsonObject js, boolean local) {
////        JsonObject rhs = js.getJsonObject("RHS");
////        JsonObject lhs = js.getJsonObject("LHS");
////        ExecuteSolr solrOpe = new ExecuteSolr(lhs.getJsonArray("columns"));
////        solrOpe.setDbName(rhs.getString("collection"));
//        // get variable values or variable IDs
//
//
////        solrOpe.setStatement(rhs.getString("statement"));
////        if (!local) {
////            Set<Integer> produce = new HashSet<>();
////            produce.add(lhs.getInt("varID"));
////            solrOpe.setOutputVar(produce);
////        }
////        return solrOpe;
//    }

    private static Set<Integer> globalVarUsed(Set<Integer> varUsed, List<Integer> bIDs, VariableTable vt) {
        Set<Integer> global = new HashSet<>();
        if (varUsed == null) {
            return null;
        }
        for (int i:varUsed) {
            if (vt.isLocal(i)) {
                bIDs.add(vt.getBlockID(i));
            }
            else {
                global.add(i);
            }
        }
        return global;
    }


    public static DBOperator createDBNode(String type, JsonObject js, VariableTable vt, boolean local) {
        DBOperator dbOperator;
        DBTypes dbt = DBTypes.valueOf(type);
        JsonObject lhs = js.getJsonObject("LHS");
        JsonObject rhs = js.getJsonObject("RHS");
        JsonArray varUsed = rhs.getJsonArray("varUsed");

        switch (dbt) {
            case AwsmCypher:
                dbOperator = new AwsmCypher();
                break;
            case SQLQuery:
                dbOperator = new ExecuteSQL();
                break;
            case Neo4jQuery:
                dbOperator = new ExecuteCypher();
                break;
            case Solr:
                dbOperator = new ExecuteSolr(lhs.getJsonArray("columns"));
                break;
            default:
                dbOperator = new AwsmSQL();
        }

        // todo: if there is varID for each local, there is no need to store parentID etc. searchVar can get the block id

        if (dbt.equals(DBTypes.SQLQuery)) {
            JsonArray tableUsed = rhs.getJsonArray("tableUsed");
            ExecuteSQL temp = new ExecuteSQL();
            if (rhs.containsKey("database")) { temp.setDbName(rhs.getString("database"));}
            temp.setTables(JsonUtil.jsonArrayToStringList(tableUsed));
            temp.setSchema(js.getJsonArray("schema"));
            dbOperator = temp;
            Map<Integer, String> awesomeTable = new HashMap<>();
        }
        else if (dbt.equals(DBTypes.Neo4jQuery)) {
            ExecuteCypher temp = new ExecuteCypher();
            if (rhs.containsKey("database")) { temp.setDbName(rhs.getString("database"));}
            // if it uses a awm graph, then need to add graph to input
            else {
                Integer graphID = rhs.getInt("graphID");
                // set graph ID and also add it to input if it is not local
                temp.setGraphID(graphID);
                if (vt.isLocal(graphID)) { temp.addBlockUsed(vt.getBlockID(graphID));}
                else {temp.addInputVariable(graphID);}
            }
            temp.setSchema(js.getJsonArray("schema"));
            dbOperator = temp;
        }
        else if (dbt.equals(DBTypes.Solr)) {
            dbOperator.setDbName(rhs.getString("collection"));
        }
        String stat = rhs.getString("statement");
        // set output variable
        if (!local) {
            Set<Integer> produce = new HashSet<>();
            produce.add(lhs.getInt("varID"));
            dbOperator.setOutputVar(produce);
        }
        // set statement if no variable used
        if (varUsed==null || varUsed.size() == 0) {
            //dbOperator.setVarDependent(false);
            dbOperator.setStatement(stat);
        }
        // if variables have constant values, should replace them with actual values
        else {
            dbOperator.setVarDependent(true);
            StringBuilder sb = new StringBuilder();
            Map<String, String> varMap = handleVar(vt, varUsed);
            Map<String, Integer> name2id = new HashMap<>();
            for (int i = 0; i < varUsed.size(); i++) {
                JsonObject tempJB = varUsed.getJsonObject(i);
                name2id.put(tempJB.getString("varName"), tempJB.getInt("varID"));
            }
            Map<Integer, Pair<Integer, Integer>>  positionMap = processSQLTemplate(stat, varMap, name2id, sb);
            Set<Integer> varWOValue = positionMap.keySet();
            if (varWOValue.size() == 0) {
                dbOperator.setVarDependent(false);
            }
            else {
                if (local) {
                    List<Integer> bIDs = new ArrayList<>();
                    varWOValue = globalVarUsed(varWOValue, bIDs, vt);
                    dbOperator.addBlocksUsed(bIDs);
                }
                dbOperator.addInputVariables(varWOValue);
            }
            dbOperator.setStatement(sb.toString());
            dbOperator.setVarPosition(positionMap);
        }
        // if awesomeSQL, the table variable should also be added
//        if (dbt.equals(DBTypes.AwsmSQL)) {
//            // get table variables
//            // if it is awsmSQL, then it stores all the varIDs of tables, if postgres, store all the string name
//            // todo: CHANGE THIS, there is no awsmSQL or awsmCypher. For an executeSQL, there can be both awsm tables and DB tables. Should be a JsonArray of JsonObjects
//            //  each of which has state about if this is in DB.
//            Set<Integer> tableVar = JsonUtil.jsonArrayToIntSet(rhs.getJsonArray("tableUsed"));
//            if (local) {
//                List<Integer> bIDs = dbOperator.getBlockUsed();
//                // if local only get global one as input, others, add blockIDs
//                tableVar = globalVarUsed(tableVar, bIDs, vt);
//                dbOperator.addBlocksUsed(bIDs);
//            }
//            dbOperator.addInputVariables(tableVar);
//        }
        return dbOperator;
    }


//    public static NER createNERNode(JsonObject js, VariableTable vt) {
//        NER ner = new NER();
//        JsonObject rhs = js.getJsonObject("RHS");
//        JsonArray varUsed = rhs.getJsonArray("parameters");
//        Set<Integer> vIDs = new HashSet<>();
//        Set<Integer> vProIDs = new HashSet<>();
//        JsonObject lhs = js.getJsonObject("LHS");
//        JsonArray varProduced = lhs.getJsonArray("variables");
//        for (int i = 0; i < varUsed.size(); i++) {
//            JsonObject temp = varUsed.getJsonObject(i);
//            if (temp.getString("variable") != null) {
//                vIDs.add(temp.getInt("varID"));
//            }
//        }
//        for (int i = 0; i < varProduced.size(); i++) {
//            JsonObject temp = varProduced.getJsonObject(i);
//            vProIDs.add(temp.getInt("varID"));
//        }
//        ner.setInputVar(vIDs);
//        ner.setOutputVar(vProIDs);
//        return ner;
//    }
    public static FunctionOperator createFuncNode(String name, JsonObject js, VariableTable vt, boolean local) {
        funcNames funcName = funcNames.valueOf(name);
        JsonObject rhs = js.getJsonObject("RHS");
        JsonArray varUsed = rhs.getJsonArray("parameters");

        FunctionOperator fop;
        switch (funcName) {
            // todo: add more functions
            case pageRank:
                fop = new PageRank(varUsed); break;
            case NER:
                fop = new NER(varUsed);
                break;
            case arrayAggregate:
                fop = new ArrayAggregate(varUsed);
                break;
            case stringJoin:
                fop = new StringJoin(varUsed);
                break;
            case stringFlat:
                fop = new StringFlat(varUsed);
                break;
//            case constructGraphFromDoc:
//                fop = new BuildGraphFromRelation(varUsed);
//                break;
            case createHistogram:
                fop = new CreateHistogram(varUsed);
                break;
            case union:
                fop = new Union(varUsed);
                break;
            case autoPhrase:
                fop = new AutoPhrase(varUsed);
                break;
            case tdm:
                fop = new Tdm(varUsed);
                break;
            case topicModel:
                fop = new TopicModel(varUsed);
                break;
            case tokenize:
                fop = new Tokenize(varUsed);
                break;
            case add:
                fop = new AddFunc(varUsed);
                break;
            case lda:
                fop = new LDA(varUsed); break;
            case sum:
                fop = new SumOpe(varUsed); break;
            case buildWordNeighborGraph:
                fop = new BuildWordNeighborGraph(varUsed);
                break;
            case getValue:
                fop = new GetTextMatrixValue(varUsed); break;
            case rowNames:
                fop = new RowNames(varUsed); break;
            case stringReplace:
                fop = new StringReplace(varUsed); break;
            case toList:
                fop = new ToList(varUsed); break;
            default:
                fop = new BlackBoxFunc(name, varUsed);
                break;
        }
        Set<Integer> vIDs = new HashSet<>();

        // set blockIDs for used local variables, and do not add them to input variables set
        if (local) {
            List<Integer> bIDs = new ArrayList<>();
            for (int i = 0; i < varUsed.size(); i++) {
                JsonObject temp = varUsed.getJsonObject(i);
                if (temp.containsKey("localVariable")) {
                    bIDs.add(temp.getInt("blockID"));
                }
                else if (temp.getString("variable").equals("true")) {
                    vIDs.add(temp.getInt("varID"));
                }
            }
            fop.setInputVar(vIDs);
            fop.setBlockUsed(bIDs);
        }
        else {
            for (int i = 0; i < varUsed.size(); i++) {
                JsonObject temp = varUsed.getJsonObject(i);
                if (temp.getString("variable").equals("true")) {
                    vIDs.add(temp.getInt("varID"));
                }
            }
            fop.setInputVar(vIDs);
            Set<Integer> vProIDs = new HashSet<>();
            JsonObject lhs = js.getJsonObject("LHS");
            JsonArray varProduced = lhs.getJsonArray("variables");
            boolean withOrder = varProduced.size()>1;
            List<Integer> varProWithOrder = new ArrayList<>();
            for (int i = 0; i < varProduced.size(); i++) {
                JsonObject temp = varProduced.getJsonObject(i);
                Integer tempID = temp.getInt("varID");
                vProIDs.add(tempID);
                if (withOrder) {
                    varProWithOrder.add(tempID);
                }
            }
            if (withOrder) {
                fop.setOutputWithOrder(varProWithOrder);
            }
            fop.setOutputVar(vProIDs);
        }
        return fop;
    }

    public static ConstructGraphFromRelation createConstructGFRNode(JsonObject js, boolean local) {
        JsonObject relationUsed = js.getJsonObject("relationUsed");
        Integer rID = relationUsed.getInt("varID");
        String rName = relationUsed.getString("varName");
        ConstructGraphFromRelation op = new ConstructGraphFromRelation(rID, rName, js.getBoolean("hasDirection"),
                js.getJsonObject("source"), js.getJsonObject("target"), js.getJsonObject("edge"));
        if (local && relationUsed.containsKey("localVariable")) {
             op.addBlockUsed(relationUsed.getInt("blockID"));
        }
        else {
            Set<Integer> input = new HashSet<>();
            input.add(rID);
            op.setInputVar(input);
        }
        return op;
    }

    public static List<Report> createReportNode(JsonObject js) {
        List<Report> nodes = new ArrayList<>();
        JsonArray var = js.getJsonArray("varReturned");
        Set<Integer> variables = JsonUtil.jsonArrayToIntSet(var);
        for (Integer varID : variables) {
            Report node = new Report();
            Set<Integer> temp = new HashSet<>();
            temp.add(varID);
            node.setInputVar(temp);
            nodes.add(node);
        }
        //op.setOutputVar(variables);
        return nodes;
    }

    public static void main(String[] args) {
        String x = "q=information-class:\\\"pulications:scholar\\\" AND text-field:weapon&rows=10";
        System.out.println(x);
    }
}

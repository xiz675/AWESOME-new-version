package edu.sdsc.queryprocessing.planner.logicalplan;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators.BiPredicate;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators.FilterExpression;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators.MapExpression;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators.ReduceExpression;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.jgrapht.*;
import org.jgrapht.graph.*;
import edu.sdsc.queryprocessing.planner.logicalplan.utils.CreateNodes;

import javax.json.JsonArray;
import javax.json.JsonObject;
import java.util.*;

public class CreatePlanDAG {

    //static final String[]  = {"constantAssign", "sqlQuery", "funcCall", "report"};

    enum types {
        ConstantAssign, SQLQuery, AwsmSQL, FuncCall, Report, Neo4jQuery, AwsmCypher, Solr, Store, ListCreation,
        VarAssign, LambdaVar, MapExpression, ReduceExpression, FilterExpression, BiOperation, ConstructGraphFromRelation
    }

    // todo: for the suboperator, if it is not a function operator, then should assign a variable name to it so that will not to run it on each element
    public static Operator getUnitOperator(JsonObject temp, VariableTable vt, boolean local) {
        // todo: add produce new to ListCreation
        Operator oprt;
        String btp = temp.getString("blockType");
        types crttype = types.valueOf(btp);
        if (crttype.equals(types.SQLQuery) || crttype.equals(types.AwsmSQL) || crttype.equals(types.AwsmCypher) || crttype.equals(types.Neo4jQuery) || crttype.equals(types.Solr)) {
            oprt = CreateNodes.createDBNode(btp, temp, vt, local);
        }
//        else if (crttype.equals(types.Solr)) {
//            oprt = CreateNodes.createSolrNode(temp, local);
//        }
        // todo: need to change store to set input variables and also indexes
        else if (crttype.equals(types.Store)) {
            JsonObject storeDetail = temp.getJsonObject("storeDetail");
            oprt = new Store(storeDetail);
//            oprt = new Store(storeDetail.getString("DBName"), storeDetail.getString("TableName"), jsonArrayToStringList(storeDetail.getJsonArray("colNames")));
        }
        else if (crttype.equals(types.VarAssign)) {
            oprt = new VarAssign(temp);
        }
        else if (crttype.equals(types.ListCreation)) {
            JsonObject detail = temp.getJsonObject("RHS");
            // todo : add input variables if there is a variable in start or end or step; also list creation can be inside a where or map, so should add a local
            oprt = new ListCreation(detail.getJsonObject("start"), detail.getJsonObject("end"), detail.getJsonObject("step"));
            if (!local) {
                Set<Integer> produce = new HashSet<>();
                produce.add(temp.getJsonObject("LHS").getInt("varID"));
                oprt.setOutputVar(produce);
            }
        }
        else if (crttype.equals(types.ConstructGraphFromRelation)) {
            oprt = CreateNodes.createConstructGFRNode(temp.getJsonObject("RHS"), local);
        }
        else if (crttype.equals(types.ConstantAssign)) {
            oprt = new ConstantAssignment(temp);
        }
        else {
            assert crttype.equals(types.FuncCall);
            String funcName = temp.getString("funcName");
            oprt = CreateNodes.createFuncNode(funcName, temp, vt, local);
        }
        return oprt;
    }

    // biOperator can only be called by a filter expression, so it should inherite the blockID of FilterExpression
    public static Operator getSubPlans(JsonObject ope, Graph<Operator, PlanEdge> g, VariableTable vt, Map<Integer, Integer> variable2Vetex, Integer vertexID, Integer... parentBlock) {
        Operator oprt;
        String btp = ope.getString("blockType");
        types crttype = types.valueOf(btp);
        if (crttype.equals(types.MapExpression) || crttype.equals(types.FilterExpression)
                || crttype.equals(types.ReduceExpression)) {
            JsonObject RHS = ope.getJsonObject("RHS");
            Integer bID = RHS.getInt("blockID");
            Operator innerOprt;
            // the input variables and list of blockIDs are set in the constructors for VALUE
            if (crttype.equals(types.MapExpression)) {
                // only when value part has global variable, add a link to g, for the inner part,
                // they will add theirs
                oprt = new MapExpression(ope.getJsonObject("mappedVariable"), ope.getJsonObject("iterateVariable"));
                // If RHS, then recursive and determine if the inner uses any local variable in this block
                innerOprt = getSubPlans(RHS, g, vt, variable2Vetex, vertexID, bID);
                ((MapExpression) oprt).setInputOperator(innerOprt.getId());
            }
            else if (crttype.equals(types.FilterExpression)) {
                // todo:  add multiple expressions
                JsonObject inner = RHS.getJsonArray("filterExpression").getJsonObject(0);
                oprt = new FilterExpression(ope.getJsonObject("filterVariable"), inner, ope.getJsonObject("iterateVar"));
                // If RHS, then recursive and determine if the inner uses any local variable in this block
                innerOprt = getSubPlans(inner, g, vt, variable2Vetex, vertexID, bID);
                ((FilterExpression) oprt).setInputOperator(innerOprt.getId());
            }
            else {
                oprt = new ReduceExpression(ope.getJsonObject("reducedVariable"), ope.getJsonObject("firstLocalVariable"), ope.getJsonObject("secondLocalVariable"));
                // If RHS, then recursive and determine if the inner uses any local variable in this block
                innerOprt = getSubPlans(RHS, g, vt, variable2Vetex, vertexID, bID);
                ((ReduceExpression) oprt).setInputOperator(innerOprt.getId());
            }
            // get the operator ID of the inner operator and then set the current operator's id
            oprt.setId(innerOprt.getId() + 1);
            // if inner Operator uses variable in the current block, then set it as an operator
            // otherwise it is a normal operator with an operator ID which pass value
            if (innerOprt.getBlockUsed().contains(bID)) {
                innerOprt.setReturnOperator();
            }
            // get all blockID the inner operator uses except its own blockID
            oprt.addBlocksUsed(innerOprt.getBlockUsed());
            // add all links from other operator in g based on input variables
            findParent(g, variable2Vetex, oprt, oprt.getInputVar());
            g.addEdge(innerOprt, oprt);
        }
        else if (crttype.equals(types.BiOperation)) {
            assert parentBlock.length == 1;
            Integer bID = parentBlock[0];
            Operator leftOpe = getSubPlans(ope.getJsonObject("leftOperatee"), g, vt, variable2Vetex, vertexID, bID);
            vertexID = leftOpe.getId();
            Operator rightOpe = getSubPlans(ope.getJsonObject("rightOperatee"), g, vt, variable2Vetex, vertexID+1, bID);
            if (leftOpe.getBlockUsed().contains(bID)) {
                leftOpe.setReturnOperator();
            }
            if (rightOpe.getBlockUsed().contains(bID)) {
                rightOpe.setReturnOperator();
            }
            oprt = new BiPredicate(ope, leftOpe.getId(), rightOpe.getId());
            // add all blocks from each
            oprt.addBlocksUsed(leftOpe.getBlockUsed());
            oprt.addBlocksUsed(rightOpe.getBlockUsed());
            // set the operator ID for this operator
            oprt.setId(rightOpe.getId() + 1);
            findParent(g, variable2Vetex, oprt, oprt.getInputVar());
            g.addEdge(leftOpe, oprt);
            g.addEdge(rightOpe, oprt);
        }
        else {
            // add blockIDList to the operator so that the outer layer can get the blocks it uses
            oprt = getUnitOperator(ope, vt, true);
            oprt.setId(vertexID);
            // link this operator to its parents in g
            findParent(g, variable2Vetex, oprt, oprt.getInputVar());
        }
        // if it was called by another operator, set a blockID, this is different from blockUsed. Every inner operator has a blockID
        // even though it may not use any variable in that block
        // if the blockID now is used by the inner side, then the inner side is an operator (function)
        // todo: this is not needed if we have outputOperatorID and inputOperatorID
        if (parentBlock.length > 0) {oprt.setBlockID(parentBlock[0]);}
        return oprt;
    }


//    public static Operator getUnitOperatorForFilter(JsonObject temp, VariableTable vt) {
//        Operator oprt;
//        String btp = temp.getString("blockType");
//        types crttype = types.valueOf(btp);
//        if (crttype.equals(types.ConstantAssign)) {
//            oprt = new ConstantAssignment(temp);
//        }
//        else if (crttype.equals((types.LambdaVar))) {
//            oprt = new LambdaVar(temp);
//        }
//        else {
//            oprt = getUnitOperator(temp, vt, true);
//        }
//        return oprt;
//    }

    public static Graph<Operator, PlanEdge> getNaivePlanDAG(JsonArray parseJson, VariableTable vt) {
        Graph<Operator, PlanEdge> planDAG = new DefaultDirectedGraph<Operator, PlanEdge>(PlanEdge.class);
        Map<Integer, Integer> variable2Vetex = new HashMap<>();
        Integer vertexID = 0;
        for (int i = 0; i < parseJson.size(); i++) {
            JsonObject temp = parseJson.getJsonObject(i);
            String btp = temp.getString("blockType");
            types crttype = types.valueOf(btp);
            if (crttype.equals(types.ConstantAssign)) {
                JsonObject lhs = temp.getJsonObject("LHS");
                if (!lhs.getString("varType").equals("List")) {
                    continue;
                }
                Operator oprt = new ConstantAssignment(lhs, temp.getJsonObject("RHS"));
                oprt.setId(vertexID++);
                Set<Integer> producedVariables = oprt.getOutputVar();
                createVarToVertexMap(producedVariables, vertexID, variable2Vetex);
                planDAG.addVertex(oprt);
            }
            // report should usually be the last expression, so no need to get the latest vertexID
            else if (crttype.equals(types.Report)) {
                List<Report> oprts = CreateNodes.createReportNode(temp);
                vertexID = constructReturnNodes(planDAG, variable2Vetex, oprts, vertexID);
            }
            // if it is a higherOrderOperator, then do it recursively.
            else if (crttype.equals(types.MapExpression) || crttype.equals(types.FilterExpression)
                        || crttype.equals(types.ReduceExpression) || crttype.equals(types.BiOperation)) {
                // todo: add constantAssign to MapExpression
                // a helper function
                Operator oprt = getSubPlans(temp, planDAG, vt, variable2Vetex, vertexID);
                // only need to set output variables, since input has been set in the helper function and links are added to varUsed
                Set<Integer> produce = new HashSet<>();
                produce.add(temp.getJsonObject("LHS").getInt("varID"));
                oprt.setOutputVar(produce);
                // add to the map with the map between variables to the operator that generate this variable
                if (produce.size() > 0) {
                    createVarToVertexMap(produce, vertexID, variable2Vetex);
                }
                vertexID = oprt.getId() + 1;
//                    JsonObject innerDetail = temp.getJsonObject("RHS");
//                    Operator innerOperator = getUnitOperator(innerDetail, vt, true);
//                    // inside the map expression, needs to add the mapped varID to a set
//                    oprt = new MapExpression(temp.getJsonObject("mappedVariable"), temp.getJsonObject("iterateVariable"), innerOperator);
//                    // the mapped var is also an input var
//                    Set<Integer> produce = oprt.getInputVar();
//                    produce.add(temp.getJsonObject("LHS").getInt("varID"));
//                    oprt.setOutputVar(produce);
//                    // set all inputVar of inner operator to Map Operator
//                    oprt.setInputVar(innerOperator.getInputVar());
                }
            // for other operators, unchanged
            else {
                Operator oprt = getUnitOperator(temp, vt, false);
                oprt.setId(vertexID);
                Set<Integer> producedVariables = oprt.getOutputVar();
                if (producedVariables.size() > 0) {
                    // map variable to the operator that generates it
                    createVarToVertexMap(producedVariables, vertexID, variable2Vetex);
                }
                vertexID += 1;
                Set<Integer> usedVar = oprt.getInputVar();
                // find parent operators which generate the used variables
                findParent(planDAG, variable2Vetex, oprt, usedVar);
                // todo: add capability On Variable or capability on operator. Given an operator, find the children from the DAG, and for each child, needs to know if
                //  this has capability on this operator.

            }

        }
        return planDAG;
    }

    private static void createVarToVertexMap(Set<Integer> variable, int vID, Map<Integer, Integer> variable2Vetex) {
        for (int i : variable) {
            variable2Vetex.put(i, vID);
        }
    }


    private static void findParent(Graph<Operator, PlanEdge> g, Map<Integer, Integer> v2n, Operator oprt, Set<Integer> usedVars) {
        g.addVertex(oprt);
        for (int v : usedVars) {
            Integer nodeID = v2n.get(v);
            Operator parent = g.vertexSet().stream().filter(opt -> (opt.getId() != null) && (opt.getId().equals(nodeID))).findAny()
                    .get();
            g.addEdge(parent, oprt);
        }
    }

    private static Integer constructReturnNodes(Graph<Operator, PlanEdge> g, Map<Integer, Integer> v2n, List<Report> reportNodes, Integer i) {
        Map<Integer, Report> node2report = new HashMap<>();
        for (Report r : reportNodes) {
            Integer usedVar = r.getInputVar().iterator().next();
            Integer nodeID = v2n.get(usedVar);
            if (node2report.containsKey(nodeID)) {
                Report prev = node2report.get(nodeID);
                Set<Integer> crtSet = prev.getInputVar();
                crtSet.add(usedVar);
                prev.setInputVar(crtSet);

            }
            else {node2report.put(nodeID, r);}
        }
        for (Integer nID : node2report.keySet()) {
            Report node = node2report.get(nID);
            node.setId(i);
            i = i+1;
            g.addVertex(node);
            Operator parent = g.vertexSet().stream().filter(opt -> (opt.getId() != null) && (opt.getId().equals(nID))).findAny()
                    .get();
            g.addEdge(parent, node);
        }
        return i;
    }



}

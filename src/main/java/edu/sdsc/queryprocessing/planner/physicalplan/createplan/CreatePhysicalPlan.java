package edu.sdsc.queryprocessing.planner.physicalplan.createplan;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.HigherLevelOperators.HighLevelOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.ConstantAssignmentPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.ListCreationPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.PhysicalOperator;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.BuildGraphFromRelationPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.graphoperators.CreateGraphPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.BiOpePhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.FilterPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.HighLevelPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.highleveloperators.MapPhysical;
import edu.sdsc.queryprocessing.planner.physicalplan.element.storage.StoreList;
import edu.sdsc.queryprocessing.planner.physicalplan.utils.*;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import edu.sdsc.utils.Pair;
import edu.sdsc.variables.logicalvariables.VariableTable;
import org.eclipse.collections.impl.block.procedure.MapCollectProcedure;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import javax.json.JsonObject;
import java.io.IOException;
import java.util.*;
import java.util.logging.Filter;
import java.util.stream.Collectors;

import static edu.sdsc.queryprocessing.planner.physicalplan.createplan.CreatePhysicalPlanWithPipeline.*;
import static edu.sdsc.queryprocessing.planner.physicalplan.utils.PlanUtils.*;

public class CreatePhysicalPlan {
    // maintain a map for function operator, if it is subOperator, keep a map of old id to new id. for HLO, get the input Operator ids and find the new ids
    // For an operator which is a function operator, keep it as a function operator
    // add an edge from subOpe to HLO
    // for the function operator, needs to keep the variable IDs (or maybe blockIDs for lambda) of local variables and for HLO
    // for without pipeline version, just choose the blocking execution and do not need to change the actual plan
    // todo: should set a dependent operator for sub-operator inside the HLOs so that the HLO will execute them in order
    // todo: hard code it for optimization now.
    public static List<List<PhysicalOperator>> getPhysicalPlan(Graph<Operator, PlanEdge> g, VariableTable vt, JsonObject config, boolean unify, boolean optimized, boolean fuse, boolean pipeline)  throws IOException {
        Graph<PhysicalOperator, PlanEdge> physicalDAG = new DefaultDirectedGraph<>(PlanEdge.class);
        Map<Pair<Integer, String>, Integer> variable2Vertex = new HashMap<>();
        Map<Integer, Integer> old2newFunctionOpe = new HashMap<>();
        List<Operator> operatorList = g.vertexSet().stream().sorted(Comparator.comparing(Operator::getId)).collect(Collectors.toList());
        int nodeID = 0;
        Integer newIntermidiateVarID = -1;
        CreateExecuteSQLNode sqlNodes = new CreateExecuteSQLNode(vt);
        CreateExecuteCypherNode cypherNode = new CreateExecuteCypherNode(vt);
        for (Operator oprt : operatorList) {
            List<PhysicalOperator> physicalOperator = new ArrayList<>();
            if (oprt instanceof  Store) {
                String vType = vt.getVarType(((Store) oprt).getStoreVarId());
                if (vType.equals("List")) {
                    physicalOperator.add(new StoreList((Store) oprt));
                }
                else {
                    // todo: add store relation and graph
                    continue;
                }
            }
            else if (oprt instanceof ExecuteSQL) {
                physicalOperator= sqlNodes.getNodes((ExecuteSQL) oprt, config, unify, newIntermidiateVarID, optimized, oprt.isSubOperator());
                // todo: add new intermediate operator
                if (physicalOperator.size() > 0) {
                    newIntermidiateVarID = physicalOperator.get(physicalOperator.size()-1).getIntermediateVarID();
                }
            }
            else if (oprt instanceof ExecuteCypher) {
                physicalOperator = cypherNode.getNodes((ExecuteCypher) oprt, unify);
            }
            else if (oprt instanceof ConstantAssignment) {
                physicalOperator.add(new ConstantAssignmentPhysical((ConstantAssignment) oprt) );
            }
            else if (oprt instanceof HighLevelOperator) {
                physicalOperator = CreateHighLevelOperatorNode.getNodes((HighLevelOperator) oprt, old2newFunctionOpe, pipeline);
            }
            else if (oprt instanceof ListCreation) {
                physicalOperator.add(new ListCreationPhysical((ListCreation) oprt));
            }
            else if (oprt instanceof ConstructGraphFromRelation) {
                PhysicalOperator collectGraph =  new BuildGraphFromRelationPhysical((ConstructGraphFromRelation) oprt);
//                collectGraph.setInputVar(physicalVar(oprt.getInputVar()));
                Set<Pair<Integer, String>> tempOutput = new HashSet<>();
                tempOutput.add(new Pair<>(newIntermidiateVarID, "*"));
                collectGraph.setOutputVar(tempOutput);
                newIntermidiateVarID = newIntermidiateVarID - 1;
                physicalOperator.add(collectGraph);
                CreateGraphPhysical createGraph = new CreateGraphPhysical(collectGraph.getOutputVar());
                // if the optimized is true, create a jgrapht graph, else create a tinkerpop graph,
                // todo: in the execution, also run the tinkerpop one (manually, needs to create multiple databases for this)
                // todo: store the relation, and then run the neo4j one outside
                if (optimized) {
                    // create jgrapht
                    createGraph.setTinkerpop(false);
                }
                // else tinkerpop
                createGraph.setOutputVar(physicalVar(oprt.getOutputVar()));
                physicalOperator.add(createGraph);
            }
            else {
                // todo: for functions which take column as input, add a getColumn physical operator
                physicalOperator = CreateFunctionNode.getNodes(oprt, newIntermidiateVarID, optimized);
                // if there are intermediate variables, update the global intermediate variable id
                if (physicalOperator.size() > 1) {
                    newIntermidiateVarID = physicalOperator.get(physicalOperator.size()-1).getIntermediateVarID();
                }
            }
            for (int i=0; i<physicalOperator.size(); i++) {
                PhysicalOperator pOprt = physicalOperator.get(i);
                pOprt.setId(nodeID);
                // if this is a function operator, it should still be
                pOprt.setReturnOperator(oprt.isReturnOperator());
                // if this is a sub operator for any HLO, add to map
                // this should be moved our from here and
                if (oprt.isSubOperator()) {
                    // if the sub operator generates several physical operators, should let the last one keep the id of oprt
                    // cause only the last one should be linked to HLOs. And the former operators are set as its dependency
                    old2newFunctionOpe.put(oprt.getId(), nodeID);
                    // if this cracked operator is not the first one, then it should be dependent on the former operator
                    if (i>0) {
                        pOprt.setDependentOpeID(nodeID-1);
                    }
                    if (i==0) {
                        pOprt.setFirstOpeInChain(true);
                        // todo: in the future, should check if the capability on var is the local var of the HLO, only when it is
                        //  local, need to change execution mode
                        // remove the capability change here and change it in the execution mode setting code
//                        pOprt.setCapability(inputStreamBlockingCap(pOprt));
                    }
                    // if this is the last one in the subOperators chain, output of subOperator has to be materialized
                    if (i==physicalOperator.size()-1) {
                        pOprt.setLastOpeInChain(true);
//                        pOprt.setCapability(outputStreamBlockingCap(pOprt));
                    }
                    // if this is subOperator, physical operator should know it
                    pOprt.setSubOperator(true);
                }

                // for each variable, set the nodeID that generate it
                Set<Pair<Integer, String>> producedVariables = pOprt.getOutputVar();
                if (producedVariables.size() > 0) {
                    createVarToVertexMap(producedVariables, nodeID, variable2Vertex);
                }
                // for HLOs, the parents are not only the operators that pass variables to it, but also the subOperators that will be applied on each element
                Set<Pair<Integer, String>> usedVar = pOprt.getInputVar();
                findParent(physicalDAG, variable2Vertex, pOprt, usedVar, old2newFunctionOpe);
//                // if this is HLO, add sub operators to it
//                if (oprt instanceof HighLevelOperator) {
//                    Set<Integer> subOperators = ((HighLevelPhysical) pOprt).getInputOperator();
//
//                }
                nodeID = nodeID + 1;
            }
        }
        // todo: process for HL operators including Map, filter and BiOpe
        List<PhysicalOperator> operations = physicalDAG.vertexSet().stream().sorted(Comparator.comparing(PhysicalOperator::getId))
                .collect(Collectors.toList());
        for (PhysicalOperator ope:operations) {
            if (ope instanceof MapPhysical) {
                // set inner operator
                MapPhysical mapOp = (MapPhysical) ope;
                mapOp.setInnerOpe(findOpeByID(operations, mapOp.getInputOperator().iterator().next()));
                mapOp.setInnerOperators(buildSubGraphForMap(operations, mapOp));
                mapOp.setLocalVarPairID(new Pair<>(mapOp.getLocalVarID().get(0), "*"));
            }
            if (ope instanceof BiOpePhysical) {
                BiOpePhysical biOpe = (BiOpePhysical) ope;
                biOpe.setLeftOpe(findOpeByID(operations, biOpe.getLeftOperatorId()));
                biOpe.setRightOpe(findOpeByID(operations, biOpe.getRightOperatorId()));
            }
            if (ope instanceof FilterPhysical) {
                FilterPhysical filterOpe = (FilterPhysical) ope;
                filterOpe.setInnerOpe(findOpeByID(operations, filterOpe.getInputOperator().iterator().next()));
            }
        }
        if (fuse) {
            fuseHLOs(physicalDAG);
        }
        List<List<PhysicalOperator>> chains;
        if (pipeline) {
            chains = setExecutionModes(physicalDAG);
        }
        else {
            chains = new ArrayList<>();
            for (PhysicalOperator p:operations) {
                if (!p.isSubOperator()) {
                    List<PhysicalOperator> tempP = new ArrayList<>();
                    tempP.add(p);
                    chains.add(tempP);
                }
            }
        }
        return chains;
    }

//
//    public static Graph<PhysicalOperator, PlanEdge> getPhysicalPlanWithMapFusion(Graph<Operator, PlanEdge> g, VariableTable vt, JsonObject config, boolean unify) throws IOException {
//        Graph<PhysicalOperator, PlanEdge> naiveDAG =  getPhysicalPlan(g, vt, config, unify, true);
//        fuseHLOs(naiveDAG);
//        return naiveDAG;
//    }


    private static void  createVarToVertexMap(Set<Pair<Integer, String>> variable, int vID, Map<Pair<Integer, String>, Integer> variable2Vetex) {
        for (Pair<Integer, String> i : variable) {
            variable2Vetex.put(i, vID);
        }
//        return variable2Vetex;
    }

    public static PhysicalOperator findOpeByID(List<PhysicalOperator> g, Integer nodeID) {
        return g.stream().filter(opt -> opt.getId().equals(nodeID)).findAny().get();
//        return g.vertexSet().stream().filter(opt -> opt.getId().equals(nodeID)).findAny()
//                .get();
    }


    // link to parent node and also add subOperator to HLOs
    static void findParent(Graph<PhysicalOperator, PlanEdge> g, Map<Pair<Integer, String>, Integer> v2n, PhysicalOperator oprt, Set<Pair<Integer, String>> usedVars, Map<Integer, Integer> old2new) {
        g.addVertex(oprt);
        // find all sub operators IDs, and add edges between subOperators to this operator
        if (oprt instanceof HighLevelPhysical) {
            Set<Integer> operatorIDs = ((HighLevelPhysical) oprt).getInputOperator();
            for (Integer opeID : operatorIDs) {
                PhysicalOperator parent = g.vertexSet().stream().filter(opt -> opt.getId().equals(opeID)).findAny()
                        .get();
                ((HighLevelPhysical) oprt).addSubOperator(parent);
                g.addEdge(parent, oprt);
            }
//            for (Integer t : oldIDs) {
//                pID.add(old2new.get(t));
//            }
        }
        for (Pair<Integer, String> v : usedVars) {
            Integer nodeID = v2n.get(v);
            PhysicalOperator parent = g.vertexSet().stream().filter(opt -> opt.getId().equals(nodeID)).findAny()
                    .get();
            g.addEdge(parent, oprt);
        }

//        return g;
    }
}

package edu.sdsc.queryprocessing.planner.logicalplan.utils;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Operator;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.Nodes.Report;
import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import org.jgrapht.Graph;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PlanDAGUtils {
    public List<Report> getReportNode(Graph<Operator, PlanEdge> graph) {
        Set<Operator> nodes = graph.vertexSet();
        List<Report> returnNodes = new ArrayList<>();
        for (Operator i : nodes) {
            if (i.isReturnNode()) {
                returnNodes.add((Report) i);
            }
        }
        if (returnNodes.size() != 0) {
            return returnNodes;
        }
        throw new IllegalArgumentException("No return variables");
    }



}

package edu.sdsc.queryprocessing.planner.physicalplan.createplan;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

import java.util.Iterator;

public class CreateTaskParallelPlan {
    private Graph<TestNode, DefaultEdge> rawPlanGraph;

    public CreateTaskParallelPlan(Graph<TestNode, DefaultEdge> g) {
        this.rawPlanGraph = g;
    }

    private Integer getParallelPlan(TestNode oprt, Integer processID, boolean... occupied) {
        if (oprt.isHasValue()) {
            if (oprt.isOccupied()) {
                oprt.increaseChildrenTouch();
            }
            return oprt.getChildrenTouch();
        }
        else {
            Iterator edges =  rawPlanGraph.incomingEdgesOf(oprt).iterator();
            if (!edges.hasNext()) {
                oprt.setHasValue(true);
                if (occupied.length == 1) {
                    oprt.setOccupied(true);
                }
                oprt.setProcessorID(processID);
                return oprt.getChildrenTouch();
            }
            else {
                if (occupied.length == 1) {
                    oprt.setOccupied(true);
                }
                Integer tempID = processID;
                Integer crtID;
                Integer newProcessID = processID;
                PlanEdge firstEdge = (PlanEdge) edges.next();
                TestNode firstNode = rawPlanGraph.getEdgeSource(firstEdge);
                crtID = getParallelPlan(firstNode, tempID, true);
                newProcessID = Math.min(newProcessID, crtID);
                tempID = Math.max(crtID, tempID);
                tempID = tempID + 1;
                while(edges.hasNext()) {
                    TestNode parent = rawPlanGraph.getEdgeSource((PlanEdge) edges.next());
                    crtID = getParallelPlan(parent, tempID);
                    newProcessID = Math.min(newProcessID, crtID);
                    tempID = Math.max(crtID, tempID);
                    tempID = tempID + 1;
                }
                oprt.setHasValue(true);
                oprt.setProcessorID(newProcessID);
                return tempID - 1;
            }
        }
    }

    public Graph<TestNode, DefaultEdge> getParallelPlanGraph(TestNode oprt, Integer processID) {
        getParallelPlan(oprt, processID);
        return rawPlanGraph;
    }

    public static void main(String[] args) {

    }

}

package edu.sdsc.queryprocessing.planner.physicalplan;

import edu.sdsc.queryprocessing.planner.logicalplan.DAGElements.PlanEdge;
import edu.sdsc.queryprocessing.planner.physicalplan.createplan.CreateTaskParallelPlan;
import edu.sdsc.queryprocessing.planner.physicalplan.createplan.TestNode;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import java.util.ArrayList;
import java.util.List;

public class Test {
    public static void main(String[] args) {
        Graph<TestNode, DefaultEdge> g = new DefaultDirectedGraph<>(PlanEdge.class);
        List<TestNode> vertexList = new ArrayList<>();
        for (int i=1; i<9; i++) {
            TestNode node = new TestNode(i);
            vertexList.add(node);
            g.addVertex(node);
        }
        g.addEdge(vertexList.get(0), vertexList.get(2));
        g.addEdge(vertexList.get(1), vertexList.get(2));
        g.addEdge(vertexList.get(2), vertexList.get(4));
        g.addEdge(vertexList.get(3), vertexList.get(4));
        g.addEdge(vertexList.get(4), vertexList.get(5));
        g.addEdge(vertexList.get(4), vertexList.get(6));
        g.addEdge(vertexList.get(5), vertexList.get(7));
        g.addEdge(vertexList.get(6), vertexList.get(7));
        CreateTaskParallelPlan planner = new CreateTaskParallelPlan(g);
        Graph<TestNode, DefaultEdge> parallelGraph = planner.getParallelPlanGraph(vertexList.get(7), 1);
    }


}

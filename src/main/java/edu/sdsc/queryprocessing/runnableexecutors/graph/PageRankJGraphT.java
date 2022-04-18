package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.AwesomeRecord;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedRelation;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import org.jgrapht.Graph;
import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleDirectedWeightedGraph;

import java.util.*;

import static edu.sdsc.utils.MapUtils.sortByValue;

public class PageRankJGraphT<T> extends AwesomeBlockRunnable<Graph<T, DefaultWeightedEdge>, List<AwesomeRecord>> {
    private boolean limitedNum = false;
    private int numReturn = 0;

    public PageRankJGraphT(Graph<T, DefaultWeightedEdge> input, int... numReturn) {
        super(input);
        if (numReturn.length > 0) {
            limitedNum = true;
            this.numReturn = numReturn[0];
        }
    }

    @Override
    public void executeBlocking() {
        Graph graph = getMaterializedInput();
        PageRank<T, DefaultWeightedEdge> pr = new PageRank<T, DefaultWeightedEdge>(graph);
        Map<T, Double> score = pr.getScores();
        List<AwesomeRecord> result = new ArrayList<>();
        if (limitedNum) {
            Map<T, Double> scoreSorted = sortByValue(score);
            int count = 0;
            for (T node : scoreSorted.keySet()) {
                if (count >= numReturn) {
                    break;
                }
                Map<String, Object> value = new HashMap<>();
                value.put("id", node);
                value.put("pagerank", score.get(node));
                result.add(new AwesomeRecord(value));
                count += 1;
            }
        }
        else {
            for (T node : score.keySet()) {
                Map<String, Object> value = new HashMap<>();
                value.put("id", node);
                value.put("pagerank", score.get(node));
                result.add(new AwesomeRecord(value));
            }
        }
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return new MaterializedRelation(this.getMaterializedOutput());
    }

    public static void main(String[] args) {
        Graph<String, DefaultWeightedEdge> graph =
                new SimpleDirectedWeightedGraph<String, DefaultWeightedEdge>
                        (DefaultWeightedEdge.class);
        graph.addVertex("vertex1");
        graph.addVertex("vertex2");
        graph.addVertex("vertex3");
        graph.addVertex("vertex4");
        graph.addVertex("vertex5");


        DefaultWeightedEdge e1 = graph.addEdge("vertex1", "vertex2");
        graph.setEdgeWeight(e1, 5);

        DefaultWeightedEdge e2 = graph.addEdge("vertex2", "vertex3");
        graph.setEdgeWeight(e2, 3);

        DefaultWeightedEdge e3 = graph.addEdge("vertex4", "vertex5");
        graph.setEdgeWeight(e3, 6);

        DefaultWeightedEdge e4 = graph.addEdge("vertex2", "vertex4");
        graph.setEdgeWeight(e4, 2);

        DefaultWeightedEdge e5 = graph.addEdge("vertex5", "vertex4");
        graph.setEdgeWeight(e5, 4);


        DefaultWeightedEdge e6 = graph.addEdge("vertex2", "vertex5");
        graph.setEdgeWeight(e6, 9);

        DefaultWeightedEdge e7 = graph.addEdge("vertex4", "vertex1");
        graph.setEdgeWeight(e7, 7);

        DefaultWeightedEdge e8 = graph.addEdge("vertex3", "vertex2");
        graph.setEdgeWeight(e8, 2);

        DefaultWeightedEdge e9 = graph.addEdge("vertex1", "vertex3");
        graph.setEdgeWeight(e9, 10);

        DefaultWeightedEdge e10 = graph.addEdge("vertex3", "vertex5");
        graph.setEdgeWeight(e10, 1);


        PageRankJGraphT<String> exe = new PageRankJGraphT<String>(graph);
        exe.executeBlocking();
        System.out.println(exe.getMaterializedOutput());

    }
}

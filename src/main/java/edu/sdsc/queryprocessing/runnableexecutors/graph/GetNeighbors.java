package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeBlockRunnable;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;

public class GetNeighbors extends AwesomeBlockRunnable<Graph, Map<Integer, Set<Integer>>> {
    private Map<Integer, Set<Integer>> neighbors = new HashMap<>();

    public GetNeighbors(Graph input) {
        super(input);
    }


//    @Override
//    public Map<Integer, Set<Integer>> getResult() {
//        return this.neighbors;
//    }

    @Override
    public void executeBlocking() {
        Graph g = getMaterializedInput();
        List<Vertex> nodes = g.traversal().V().toList();
        for (Vertex n : nodes) {
            List<Integer> outNeighbor = g.traversal().V().hasId(n.id()).out().map(i -> ((Long) i.get().id()).intValue()).toList();
            List<Integer> inNeighbor = g.traversal().V().hasId(n.id()).in().map(i -> ((Long) i.get().id()).intValue()).toList();
            Set<Integer> neighbors = new HashSet<>();
            neighbors.addAll(outNeighbor);
            neighbors.addAll(inNeighbor);
            this.neighbors.put(((Long) n.id()).intValue(), new HashSet<>(neighbors));
        }
        this.setMaterializedOutput(Collections.unmodifiableMap(this.neighbors));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return null;
    }
}

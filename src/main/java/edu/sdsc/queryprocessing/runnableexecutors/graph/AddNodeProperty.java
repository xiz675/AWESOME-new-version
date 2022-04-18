package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomeStreamInputRunnable;
import edu.sdsc.utils.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.List;
import java.util.stream.Stream;

// this is a sequential operator since graph does not support concurrent changes, so no producer
public class AddNodeProperty<T> extends AwesomeStreamInputRunnable<Pair<Integer, T>, Graph> {
    private Graph graph;
    private String propertyName;

    public AddNodeProperty(PipelineMode mode) {
        super(mode);
    }

    // stream input
    public AddNodeProperty(String name, Graph g, Stream<Pair<Integer, T>> input) {
        super(input);
        this.graph = g;
        this.propertyName = name;
    }

    // block
    public AddNodeProperty(String name, Graph g, List<Pair<Integer, T>> input) {
        super(input);
        this.graph = g;
        this.propertyName = name;
    }

    @Override
    public void executeStreamInput() {
        Stream<Pair<Integer, T>> input = getStreamInput();
        GraphTraversalSource g = graph.traversal();
        input.forEach(node -> g.V(Long.valueOf(node.first)).property(propertyName, node.second).next());
        try {
            g.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void executeBlocking() {
        List<Pair<Integer, T>> input = getMaterializedInput();
        GraphTraversalSource g = graph.traversal();
        for (Pair<Integer, T> node : input) {
            g.V(Long.valueOf(node.first)).property(propertyName, node.second).next();
        }
        try {
            g.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return null;
    }

//    @Override
//    public Graph getResult() {
//        return graph;
//    }
}

package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.*;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedGraphData;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamGraphData;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.sdsc.utils.Pair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectGraphElementFromListOfPairs extends AwesomePipelineRunnable<List<Pair<String, String>>, GraphElement> {

    // this is for creating multiple instance for parallel execution
    public CollectGraphElementFromListOfPairs(PipelineMode mode) {
        super(mode);
    }

    // for blocking mode
    public CollectGraphElementFromListOfPairs(List<List<Pair<String, String>>> data, boolean materialize) {
        super(data, materialize);
    }

    // for streamInput mode
    public CollectGraphElementFromListOfPairs(Stream<List<Pair<String, String>>> data) {
        super(data);
    }

    // for streamOutput
    public CollectGraphElementFromListOfPairs(List<List<Pair<String, String>>> data) {
        super(data);
        setExecutionMode(PipelineMode.streamoutput);
    }

    // pipeline
    public CollectGraphElementFromListOfPairs(Stream<List<Pair<String, String>>> data, boolean streamOut) {
        super(data, streamOut);
    }


    @Override
    public void executeStreamInput() {
        Stream<List<Pair<String, String>>> input = getStreamInput();
        Stream<GraphElement> streamResult = input.flatMap(Collection::stream).map(i -> createGraphElementFromPair(i, 1));
        setMaterializedOutput(streamResult.collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        Stream<GraphElement> streamResult = getMaterializedInput().stream().flatMap(Collection::stream).map(i -> createGraphElementFromPair(i, 1));
        setStreamOutput(streamResult);
    }

    @Override
    public void executePipeline() {
        Stream<List<Pair<String, String>>> input = getStreamInput();
        Stream<GraphElement> materializedResult = input.flatMap(Collection::stream).map(i -> createGraphElementFromPair(i, 1));
        setStreamOutput(materializedResult);
    }

    @Override
    public void executeBlocking() {
        List<List<Pair<String, String>>> materializedInput = getMaterializedInput();
        List<GraphElement> materializedResult = new ArrayList<>();
        for (List<Pair<String, String>> pair: materializedInput ) {
            for (Pair<String, String> t : pair) {
                materializedResult.add(createGraphElementFromPair(t, 1));
            }
        }
        setMaterializedOutput(materializedResult);
//        System.out.println(materializedResult.size());
//        System.out.println(String.format("collect cooccurance costs: %d ms",  System.currentTimeMillis() - start));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamGraphData(this.getStreamResult());
        }
        else {
            return new MaterializedGraphData(this.getMaterializedOutput());
        }
    }

    private Edge createGraphElementFromPair(Pair<String, String> p, int count) {
        Node t1 = new Node("word", "entity", p.first);
        Node t2 = new Node("word", "entity", p.second);
        Edge e = new Edge("cooccur", "count", count);
        e.setNodes(t1, t2, false);
        return e;
    }


}

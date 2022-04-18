package edu.sdsc.queryprocessing.runnableexecutors.graph;

import edu.sdsc.datatype.execution.*;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.*;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.sdsc.utils.Pair;
import org.checkerframework.checker.units.qual.A;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectGraphElementFromDocs extends AwesomePipelineRunnable<Document, AwesomeRecord> {
    private int distance = 3;
    private List<String> words = null;
//    private ExecutionVariableTable evt;

    // for parallel
    public CollectGraphElementFromDocs(PipelineMode mode, int distance, List<String> words) {
        this.distance = distance;
        setExecutionMode(mode);
        this.words = words;
    }

    // for blocking mode
    public CollectGraphElementFromDocs(List<Document> relation, int distance, List<String> words, boolean block) {
        super(relation, block);
        this.distance = distance;
        this.words = words;
    }

    // for streamInput mode
    public CollectGraphElementFromDocs(Stream<Document> relation, int distance, List<String> words) {
        super(relation);
        this.distance = distance;
        this.words = words;
    }

    // for streamOutput
    public CollectGraphElementFromDocs(List<Document> relation, int distance, List<String> words) {
        super(relation);
        this.distance = distance;
        this.words = words;
    }

    // pipeline
    public CollectGraphElementFromDocs(Stream<Document> relation, int distance, List<String> words, boolean pipeline) {
        super(relation, pipeline);
        this.distance = distance;
        this.words = words;
    }


    @Override
    public void executeStreamInput() {
        Stream<Document> input = getStreamInput();
        Stream<AwesomeRecord> result = input.map(i -> getNeighbor(i.tokens, this.distance)).flatMap(List::stream);
        setMaterializedOutput(result.collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        List<Document> input = getMaterializedInput();
        Stream<AwesomeRecord> result = input.stream().map(i -> getNeighbor(i.tokens, this.distance)).flatMap(List::stream);
        setStreamOutput(result);
    }

    @Override
    public void executePipeline() {
        Stream<Document> input = getStreamInput();
        Stream<AwesomeRecord> result = input.map(i -> getNeighbor(i.tokens, this.distance)).flatMap(List::stream);
        setStreamOutput(result);
    }

    @Override
    public void executeBlocking() {
        List<Document> input = getMaterializedInput();
        List<AwesomeRecord> output = new ArrayList<>();
        for (Document r: input) {
            List<AwesomeRecord> t = getNeighbor(r.tokens, this.distance);
            output.addAll(t);
        }
        setMaterializedOutput(output);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamRelation(this.getStreamResult());
        }
        else {
            return new MaterializedRelation(this.getMaterializedOutput());
        }
    }


    //todo: maybe add  merge graph element to get weighted edge weights when constructing graph
    private List<AwesomeRecord> getNeighbor(List<String> tokens, Integer distance) {
        List<String> words = this.words;
        Map<Pair<String, String>, Integer> wordsPair = new HashMap<>();
        List<AwesomeRecord> results = new ArrayList<>();
        for(int i=0; i < tokens.size(); i++) {
            String w = tokens.get(i);
            if (words!=null && !words.contains(w)) {
                continue;
            }
            for (int j=i+1; j <=Math.min(tokens.size()-1, i+distance); j++) {
                String wj = tokens.get(j);
                if (words!=null && !words.contains(wj)) {continue;}
                Pair<String, String> tempPair = new Pair<>(w, wj);
                if (wordsPair.containsKey(tempPair)) {
                    wordsPair.put(tempPair, wordsPair.get(tempPair) + 1);
                }
                else {
                    wordsPair.put(tempPair, 1);
                }
            }
        }
//        String[] cols = {"word1", "word2", "count"};
//        List<AwesomeRecord> results = new ArrayList<>();
        for (Pair<String, String> crtWords : wordsPair.keySet()) {
            Pair<Pair<String, String>, Integer> value = new Pair<>(crtWords, wordsPair.get(crtWords));
            results.add(new AwesomeRecord(value));
        }
        return results;
    }

    private static List<GraphElement> mergeGraphData(List<GraphElement> graph) {
        HashMap<GraphElement, GraphElement> mergedGraph = new HashMap<>();
        graph.forEach(i -> {if (mergedGraph.containsKey(i)) {
            int count = (Integer) mergedGraph.get(i).getProperty("count");
            mergedGraph.get(i).addProperty("count", count+1);
        }
        else {
            mergedGraph.put(i, i); } });
        return new ArrayList<>(mergedGraph.values());
    }

    public void setWords(List<String> words) {
        this.words = words;
    }
}

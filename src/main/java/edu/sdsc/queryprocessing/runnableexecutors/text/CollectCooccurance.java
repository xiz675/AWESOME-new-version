package edu.sdsc.queryprocessing.runnableexecutors.text;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;
import edu.sdsc.utils.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectCooccurance extends AwesomePipelineRunnable<List<String>, List<Pair<String, String>>> {

    public CollectCooccurance(PipelineMode mode) {
        super(mode);
    }

    // for blocking mode
    public CollectCooccurance(List<List<String>> data, boolean materializeOut) {
        super(data, materializeOut);
    }

    // for streamInput mode
    public CollectCooccurance(Stream<List<String>> data) {
        super(data);
    }

    // for streamOutput
    public CollectCooccurance(List<List<String>> data) {
        super(data);
    }

    // pipeline
    public CollectCooccurance(Stream<List<String>> data, boolean streamOut) {
        super(data, true);
        assert streamOut;
    }


    @Override
    public void executeStreamInput() {
        Stream<List<Pair<String, String>>> result = getStreamInput().map(this::execute);
        setMaterializedOutput(result.collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        Stream<List<Pair<String, String>>> result = getStreamInput().map(this::execute);
        setStreamOutput(result);
    }

    @Override
    public void executePipeline() {
        Stream<List<Pair<String, String>>> result = getStreamInput().map(this::execute);
        setStreamOutput(result);
    }

    @Override
    public void executeBlocking() {
//        long start = System.currentTimeMillis();
        List<List<String>> materializedInput = getMaterializedInput();
        List<List<Pair<String, String>>> materializedResult = new ArrayList<>();
        for (List<String> r: materializedInput) {
            materializedResult.add(execute(r));
        }
        setMaterializedOutput(materializedResult);
//        System.out.println(String.format("collect cooccurance costs: %d ms",  System.currentTimeMillis() - start));
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        return null;
    }

    // executor function is the unit function for each element
    private List<Pair<String, String>> execute(List<String> s) {
        List<Pair<String, String>> currentPair = new ArrayList<>();
        List<String> listDistinct = s.stream().distinct().collect(Collectors.toList());
        for (int i=0; i<listDistinct.size(); i++) {
            for (int j=i+1; j < listDistinct.size(); j++) {
                currentPair.add(new Pair<>(listDistinct.get(i), listDistinct.get(j)));
            }
        }
        return currentPair;
    }
}

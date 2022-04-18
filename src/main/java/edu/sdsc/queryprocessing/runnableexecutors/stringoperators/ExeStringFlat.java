package edu.sdsc.queryprocessing.runnableexecutors.stringoperators;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.MaterializedList;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.StreamList;
import edu.sdsc.queryprocessing.runnableexecutors.baserunnable.AwesomePipelineRunnable;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

// this only accepts value, not operator. The information inside operator needs to be parsed outside this constructor
public class ExeStringFlat extends AwesomePipelineRunnable<String, String> {
    private String stringValue = null;
    // for parallel one
    public ExeStringFlat(PipelineMode mode, String string) {
        setExecutionMode(mode);
        this.stringValue = string;
    }

    // stream in
    public ExeStringFlat(Stream<String> input, String string) {
        super(input);
        this.stringValue = string;
    }

    // stream out
    public ExeStringFlat(List<String> input, String string) {
        super(input);
        this.stringValue = string;
    }

    // blocking
    public ExeStringFlat(List<String> input, String string, boolean materializeOut) {
        super(input, true);
        assert materializeOut;
        this.stringValue = string;
    }

    // pipeline
    public ExeStringFlat(Stream<String> input, String string, boolean streamOut) {
        super(input, true);
        this.stringValue = string;
        assert streamOut;
    }

    @Override
    public void executeBlocking() {
        List<String> input = getMaterializedInput();
        List<String> result = new ArrayList<>();
        for (String i : input) {
            result.add(executor(i));
        }
        setMaterializedOutput(result);
    }

    @Override
    public ExecutionTableEntry createTableEntry() {
        if (this.isStreamOut()) {
            return new StreamList<String>(this.getStreamResult());
        }
        else {
            return new MaterializedList<String>(this.getMaterializedOutput());
        }
    }

    @Override
    public void executeStreamInput() {
        Stream<String> output = getStreamInput().map(i -> executor(i));
        setMaterializedOutput(output.collect(Collectors.toList()));
    }

    @Override
    public void executeStreamOutput() {
        Stream<String> output = getMaterializedInput().stream().map(i -> executor(i));
        setStreamOutput(output);
    }

    @Override
    public void executePipeline() {

    }

    private String executor(String replace) {
        return stringValue.replace("$", replace);
    }

}

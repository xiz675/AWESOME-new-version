package edu.sdsc.queryprocessing.runnableexecutors.baserunnable;

import edu.sdsc.datatype.execution.PipelineMode;

import java.util.List;
import java.util.stream.Stream;

public abstract class AwesomePipelineRunnable<I, O> extends AwesomeRunnable<List<I>, List<O>>{
    private Stream<I> streamInput;
//    private List<O> materializedResult;
    private Stream<O> streamResult;

    public AwesomePipelineRunnable() {
    }

    public AwesomePipelineRunnable(PipelineMode mode) {
        super(mode);
    }

    // stream in constructor
    public AwesomePipelineRunnable(Stream<I> data) {
        super(PipelineMode.streaminput);
        setStreamInput(data);
    }

    // stream out constructor
    public AwesomePipelineRunnable(List<I> data) {
        super(PipelineMode.streamoutput);
        setMaterializedInput(data);
    }

    // pipeline constructor:
    public AwesomePipelineRunnable(Stream<I> data, boolean streamOut) {
        super(PipelineMode.pipeline);
        assert streamOut;
        this.streamInput = data;
    }

    // blocking constructor:
    public AwesomePipelineRunnable(List<I> input, boolean materializeOut) {
        super(PipelineMode.block);
        assert materializeOut;
        setMaterializedInput(input);
    }

    public void setStreamInput(Stream<I> input) {
        this.streamInput = input;
    }

    public void setStreamOutput(Stream<O> output) {
        this.streamResult = output;
    }

    public Stream<O> getStreamResult() {
        return streamResult;
    }

    public Stream<I> getStreamInput() {
        return streamInput;
    }




    @Override
    public void run() {
        long startTime;
        long endTime;
        PipelineMode executionMode = getExecutionMode();
        if (executionMode.equals(PipelineMode.streaminput)) {
            executeStreamInput();
        }
        else if (executionMode.equals(PipelineMode.streamoutput)) {
            executeStreamOutput();
        }
        else if (executionMode.equals(PipelineMode.pipeline)) {
            executePipeline();
        }
        else {
            assert executionMode.equals(PipelineMode.block);
            executeBlocking();
        }

    }


    public abstract void executeStreamInput();

    public abstract void executeStreamOutput();

    public abstract void executePipeline();

}

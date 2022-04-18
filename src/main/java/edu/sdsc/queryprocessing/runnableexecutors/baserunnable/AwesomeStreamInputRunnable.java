package edu.sdsc.queryprocessing.runnableexecutors.baserunnable;

import edu.sdsc.datatype.execution.PipelineMode;

import java.util.List;
import java.util.stream.Stream;

// it does not need latch since it can't be parallel executed
public abstract class AwesomeStreamInputRunnable<I, O> extends AwesomeRunnable<List<I>, O> {
    private Stream<I> streamInput;
//    private O result;

    public AwesomeStreamInputRunnable(PipelineMode mode) {
        super(mode);
    }
    // stream in constructor
    public AwesomeStreamInputRunnable(Stream<I> input) {
        super(PipelineMode.streaminput);
        this.setStreamInput(input);
    }

    // blocking constructor
    // for streamOut, it can not be executed in parallel, so no need to have latch
    public AwesomeStreamInputRunnable(List<I> input) {
        super(PipelineMode.block);
        this.setMaterializedInput(input);
    }

    public Stream<I> getStreamInput() {
        return streamInput;
    }

    public void setStreamInput(Stream<I> input) {
        this.streamInput = input;
    }

//
//    public abstract O getResult();

    @Override
    public void run() {
        PipelineMode executionPipelineMode = getExecutionMode();
        if (executionPipelineMode.equals(PipelineMode.streaminput)) {
            executeStreamInput();
        }
        else {
            assert executionPipelineMode.equals(PipelineMode.block);
            executeBlocking();
        }
    }

    public abstract void executeStreamInput();


}

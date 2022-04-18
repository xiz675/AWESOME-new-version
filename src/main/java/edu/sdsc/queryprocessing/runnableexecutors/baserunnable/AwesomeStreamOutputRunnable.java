package edu.sdsc.queryprocessing.runnableexecutors.baserunnable;

import edu.sdsc.datatype.execution.PipelineMode;

import java.util.List;
import java.util.stream.Stream;

// materialized result is List<O>, and stream result is Stream<List<O>>.
public abstract class AwesomeStreamOutputRunnable<I, O> extends AwesomeRunnable<I, List<O>> {
    private Stream<O> streamResult;

    public AwesomeStreamOutputRunnable(PipelineMode mode) {
        super(mode);
    }


    // stream out constructor
    public AwesomeStreamOutputRunnable(I data) {
        super(PipelineMode.streamoutput);
        // this.setLatch(latch);
        setMaterializedInput(data);
    }

    // blocking constructor:
    // for streamOut, it can not be executed in parallel, so no need to have latch
    public AwesomeStreamOutputRunnable(I input, boolean materializeOut) {
        super(PipelineMode.block);
        setMaterializedInput(input);
        assert materializeOut;
//        this.executionPipelineMode = PipelineMode.block
    }

//    public void setMaterializedResult(List<O> materializedResult) {
//        this.materializedResult = materializedResult;
//    }

    public Stream<O> getStreamResult() {
        return streamResult;
    }

    public void setStreamResult(Stream<O> streamResult) {
        this.streamResult = streamResult;
    }

    //    public I getMaterializedInput() {
//        return materializedInput;
//    }
//
//    public List<O> getMaterializedResult() {
//        return materializedResult;
//    }

    @Override
    public void run() {
        PipelineMode executionPipelineMode = getExecutionMode();
        if (executionPipelineMode.equals(PipelineMode.streamoutput)) {
            executeStreamOutput();
        }
        else {
            assert executionPipelineMode.equals(PipelineMode.block);
            executeBlocking();
        }

    }
    public abstract void executeStreamOutput();

}

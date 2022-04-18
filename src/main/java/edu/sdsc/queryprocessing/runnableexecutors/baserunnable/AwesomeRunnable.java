package edu.sdsc.queryprocessing.runnableexecutors.baserunnable;

import edu.sdsc.datatype.execution.PipelineMode;
import edu.sdsc.queryprocessing.executor.execution.elementvariable.ExecutionTableEntry;

import java.util.concurrent.CountDownLatch;

// todo: first finish the pipeline runnable and later work on others
public abstract class AwesomeRunnable<I, O> implements Runnable {
    private CountDownLatch latch;
    private PipelineMode executionMode = PipelineMode.block;
    private volatile boolean cancel = false;
    // for any extended runnable, they all have materialized input and output since they all have a blocking execution mode
    private I materializedInput;
    private O materializedOutput;

    public AwesomeRunnable() {}

    public AwesomeRunnable(PipelineMode mode) {
        this.executionMode = mode;
    }

    // stream in constructor
    public AwesomeRunnable(CountDownLatch latch) {
        this.latch = latch;
    }

    public PipelineMode getExecutionMode() {
        return executionMode;
    }

    public CountDownLatch getLatch() {
        return latch;
    }


    public boolean isCancel() {
        return cancel;
    }

    public void setCancel(boolean cancel) {
        this.cancel = cancel;
    }

    public void countDown() {
        this.latch.countDown();
    }


    public void setLatch(CountDownLatch latch) {
        this.latch = latch;
    }

    public void setExecutionMode(PipelineMode executionMode) {
        this.executionMode = executionMode;
    }

    public abstract void executeBlocking();
//
//    public CountDownLatch getProducer() {
//        return producer;
//    }
//
//    public void setProducer(CountDownLatch producer) {
//        this.producer = producer;
//    }

    public void setMaterializedOutput(O materializedOutput) {
        this.materializedOutput = materializedOutput;
    }

    public abstract ExecutionTableEntry createTableEntry();

    public O getMaterializedOutput() {
        return materializedOutput;
    }

    public void setMaterializedInput(I materializedInput) {
        this.materializedInput = materializedInput;
    };

    public I getMaterializedInput() {
        return materializedInput;
    }

    //    public abstract void setMaterializedResult(O materializedResult);
//


    public boolean isStreamIn() {
        PipelineMode mode = getExecutionMode();
        return mode.equals(PipelineMode.streaminput) || mode.equals(PipelineMode.pipeline);
    }

    public boolean isStreamOut() {
        PipelineMode mode = getExecutionMode();
        return mode.equals(PipelineMode.pipeline) || mode.equals(PipelineMode.streamoutput);
    }

}
